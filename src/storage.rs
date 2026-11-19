        }
        let next_table_id = u64::from(table_count).checked_add(1)
            .ok_or_else(|| DbError::Storage("PDB6 table ID space exhausted".into()))?;

        for (table_offset, (table_name, table)) in self.tables.iter().enumerate() {
            let table_id = u64::try_from(table_offset + 1)
                .map_err(|_| DbError::Storage("PDB6 table ID space exhausted".into()))?;
            let catalog_indexes = table.indexes.iter().map(|(name, index)| {
                let columns = index.column_indices.iter()
                    .map(|column| u32::try_from(*column)
                        .map_err(|_| DbError::Constraint("PDB6 index column ordinal exceeds u32".into())))
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(CatalogIndex { name: name.clone(), columns, unique: index.unique })
            }).collect::<Result<Vec<_>, DbError>>()?;
            let catalog = CatalogTable {
                table_id,
                name: table_name.clone(),
                columns: table.columns.iter().map(|column| CatalogColumn {
                    name: column.name.clone(),
                    data_type: column.data_type.clone(),
                    nullable: column.nullable,
                }).collect(),
                indexes: catalog_indexes,
                next_row_id: table.next_row_id,
            };
            let catalog_key = pdb6::table_key(table_id)?;
            let encoded_catalog = catalog.encode()?;
            store.apply_batch(
                [(catalog_key.clone(), encoded_catalog.clone())],
                std::iter::empty::<Vec<u8>>(),
            )?;
            let stored_catalog = store.get(&catalog_key)?
                .ok_or_else(|| DbError::Storage(format!("PDB6 migration lost catalog for table {table_name}")))?;
            if CatalogTable::decode(&stored_catalog)? != catalog {
                return Err(DbError::Storage(format!("PDB6 migration catalog verification failed for table {table_name}")));
            }

            for chunk in table.rows.chunks(5_000) {
                let encoded_rows = chunk.iter().map(|row| {
                    Ok((table_id, row.row_id, sql_rows::SqlRowStore::encode_values(&row.values, table.columns.len())?))
                }).collect::<Result<Vec<_>, DbError>>()?;
                if !encoded_rows.is_empty() {
                    store.apply_row_batch(
                        encoded_rows.clone(),
                        std::iter::empty::<(Vec<u8>, Vec<u8>)>(),
                        std::iter::empty::<Vec<u8>>(),
                    )?;
                    for (stored_table_id, row_id, expected_payload) in encoded_rows {
                        let actual_payload = store.get_row_payload(stored_table_id, row_id)?
                            .ok_or_else(|| DbError::Storage(format!(
                                "PDB6 migration lost row {row_id} in table {table_name}"
                            )))?;
                        if actual_payload != expected_payload {
                            return Err(DbError::Storage(format!(
                                "PDB6 migration row verification failed for table {table_name}, row {row_id}"
                            )));
                        }
                    }
                }
            }

            // Rebuild index entries from stable row IDs, not vector positions.
            // Flush bounded chunks so migration memory stays independent of index size.
            for (index_offset, index) in table.indexes.values().enumerate() {
                let index_id = u64::try_from(index_offset + 1)
                    .map_err(|_| DbError::Storage("PDB6 index ID space exhausted".into()))?;
                let mut pending = Vec::with_capacity(5_000);
                for row in &table.rows {
                    let key = IndexKey::from_columns(&row.values, &index.column_indices);
                    let encoded_key = index_codec::encode_index_key(&key)?;
                    let mut disk_key = pdb6::index_key(table_id, index_id, &encoded_key)?;
                    disk_key.extend_from_slice(&row.row_id.to_be_bytes());
                    if disk_key.len() > 1024 {
                        return Err(DbError::Constraint(format!(
                            "index key for table {table_name} exceeds the PDB6 key limit"
                        )));
                    }
                    pending.push((disk_key, Vec::new()));
                    if pending.len() == 5_000 {
                        let batch = std::mem::take(&mut pending);
                        let keys = batch.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                        store.apply_batch(batch, std::iter::empty::<Vec<u8>>())?;
                        for key in keys {
                            if store.get(&key)?.is_none() {
                                return Err(DbError::Storage(format!(
                                    "PDB6 migration index verification failed for table {table_name}"
                                )));
                            }
                        }
                        pending = Vec::with_capacity(5_000);
                    }
                }
                if !pending.is_empty() {
                    let keys = pending.iter().map(|(key, _)| key.clone()).collect::<Vec<_>>();
                    store.apply_batch(pending, std::iter::empty::<Vec<u8>>())?;
                    for key in keys {
                        if store.get(&key)?.is_none() {
                            return Err(DbError::Storage(format!(
                                "PDB6 migration index verification failed for table {table_name}"
                            )));
                        }
                    }
                }
            }
        }

        // The manifest is the completion marker and is published last. If a
        // process stops earlier, PDB5 is still untouched and migration can be
        // restarted after the incomplete PDB6 sidecar is removed by the operator.
        // Verify the complete table catalog before publishing the completion marker.
        let stored_table_keys = store.scan_prefix(&[pdb6::KEY_TABLE], table_count as usize)?;
        if stored_table_keys.len() != table_count as usize {
            return Err(DbError::Storage("PDB6 migration table-count verification failed".into()));
        }

        store.set_manifest(DatabaseManifest {
            generation: self.wal_sequence,
            last_tx_id: 0,
            next_table_id,
            table_count,
        })?;
        Ok(())
    }
}

/// Rebuild the durable EmberTree index mirror from the SQL-WAL-recovered state.
/// SQL snapshots/WAL remain authoritative; this operation is idempotent and repairable.
impl Database {
    pub(crate) fn sync_ember_indexes_journal(
        &self,
        journal: &[super::EmberIndexDelta],