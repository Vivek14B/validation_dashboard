def process_uploaded_file(uploaded_file, selected_date=None):
    try:
        # --- Get User Context (from your code) ---
        user_role = st.session_state.get("role")
        username = st.session_state.get("username_actual")
        managed_users = st.session_state.get("managed_users", [])

        df_original = pd.DataFrame()
        with st.spinner(f"📖 Reading file: {uploaded_file.name}..."):
            try:
                df_original = pd.read_excel(uploaded_file, engine='openpyxl', skiprows=5, skipfooter=1)
                df_original.columns = df_original.columns.str.strip()
            except Exception as e:
                st.markdown(f'<div class="error-box"><strong>❌ Error!</strong> Could not read Excel file "{uploaded_file.name}". Details: {str(e)}</div>', unsafe_allow_html=True)
                return

        if df_original.empty:
            st.markdown(f'<div class="warning-box"><strong>⚠ Warning!</strong> The uploaded file "{uploaded_file.name}" is empty or could not be parsed.</div>', unsafe_allow_html=True)
            return
            
        if 'Created user' not in df_original.columns:
            st.error("CRITICAL ERROR: The uploaded file must contain a 'Created user' column to be processed.")
            return

        # --- Role-Based Filtering (from your code) ---
        df_to_process = pd.DataFrame()
        df_original['Created user'] = df_original['Created user'].astype(str)

        if user_role == 'User':
            df_to_process = df_original[df_original['Created user'].str.lower() == username.lower()].copy()
            filter_message = f"As a **User**, this file has been automatically filtered to process records created by you."
        elif user_role == 'Manager':
            accessible_users = [username.lower()] + [u.lower() for u in st.session_state.get("managed_users", [])]
            df_to_process = df_original[df_original['Created user'].str.lower().isin(accessible_users)].copy()
            filter_message = f"As a **Manager**, this file has been filtered for you and your team."
        else: # Management and Super User
            df_to_process = df_original.copy()
            filter_message = "As **Management/Super User**, all records in the file will be processed."
        
        st.info(f"""
        {filter_message}\n
        - **{len(df_original)}** records found in the original file.
        - **{len(df_to_process)}** records to be initially processed.
        """)

        if df_to_process.empty:
            st.warning("No records in the uploaded file match your user profile or team. Nothing to process.")
            return

        # --- Check for essential columns (from your code) ---
        required_columns_for_processing = ['Department.Name', 'Account2.Code', 'Sub Ledger.Code']
        missing_core_cols = [col for col in required_columns_for_processing if col not in df_to_process.columns]
        if missing_core_cols:
            st.error(f'Error! Missing essential columns for processing: {", ".join(missing_core_cols)}. Cannot proceed.')
            return
        
        # --- NEW DUPLICATE CHECK LOGIC ---
        with st.spinner("🔍 Checking for duplicate transactions from previous runs..."):
            historical_fingerprints = db_manager.get_historical_fingerprints()
            
            validator = DataValidator(base_ref_path="reference_data")
            # 1. Run validation on ALL incoming data first
            all_exceptions_df, _ = validator.validate_dataframe(df_to_process.copy())
            
            # 2. Separate the incoming data into two groups
            exception_indices = all_exceptions_df.index
            exceptions_to_process = df_to_process.loc[exception_indices].copy()
            clean_df_from_upload = df_to_process.drop(index=exception_indices).copy()

            # 3. Filter the CLEAN rows to remove historical duplicates
            new_clean_rows = []
            ignored_clean_count = 0
            for index, row in clean_df_from_upload.iterrows():
                # ... inside the loop `for index, row in clean_df_from_upload.iterrows():` ...
                doc_no = str(row.get("Document No.", "")).strip().lower()
                location = str(row.get("Location.Name", "")).strip().lower()
                activity = str(row.get("Activity.Name", "")).strip().lower()
                crop = str(row.get("Crop.Name", "")).strip().lower()
                
                # --- NORMALIZED FINGERPRINT ---
                try:
                    net_amount_val = float(row.get("Net amount", 0.0))
                    net_amount = f"{net_amount_val:.2f}"
                except (ValueError, TypeError):
                    net_amount = "0.00"
                
                fingerprint = f"{doc_no}|{location}|{activity}|{crop}|{net_amount}"

                if fingerprint in historical_fingerprints:
                    ignored_clean_count += 1
                else:
                    new_clean_rows.append(row)
            
            if new_clean_rows:
                final_clean_df = pd.DataFrame(new_clean_rows, columns=clean_df_from_upload.columns)
            else:
                final_clean_df = pd.DataFrame(columns=clean_df_from_upload.columns)

            # 4. Re-assemble the final dataframe for processing
            final_df_to_process = pd.concat([exceptions_to_process, final_clean_df], ignore_index=True)
        
        st.success(f"Duplicate check complete. Ignored **{ignored_clean_count}** clean rows that were duplicates of past transactions.")
        st.info(f"Processing **{len(final_df_to_process)}** unique transactions (**{len(exceptions_to_process)}** with exceptions, **{len(final_clean_df)}** new clean rows).")
        # --- END OF NEW DUPLICATE CHECK LOGIC ---

        # --- Save the main validation run ---
        current_run_id = db_manager.save_validation_run(
            filename=uploaded_file.name,
            total_records=len(final_df_to_process),
            total_exceptions=len(exceptions_to_process),
            file_size=uploaded_file.size,
            upload_time=selected_date
        )

        # --- Suspicious Transaction Check (from your code, now runs on de-duplicated data) ---
        processing_log = [] 
        with st.spinner(f"🕵️‍♀️ Checking for suspicious transactions..."):
            immunity_list = db_manager.load_suspense_immunity_list()
            suspicious_rules_df = db_manager.get_all_suspicious_rules()
            flagged_count = 0

            if not suspicious_rules_df.empty:
                rules_dict = {}
                for _, rule in suspicious_rules_df.iterrows():
                    if rule['rule_values']:
                        key = (rule['sub_department_name'], rule['rule_column'])
                        rules_dict[key] = [str(v).lower() for v in rule['rule_values']]

                for index, row in final_df_to_process.iterrows(): # MODIFIED: Runs on de-duplicated data
                    user = row.get('Created user', 'Unknown User')
                    
                                        # ... inside the loop `for index, row in final_df_to_process.iterrows():` ...
                    doc_no_s = str(row.get("Document No.", "")).strip().lower()
                    location_s = str(row.get("Location.Name", "")).strip().lower()
                    activity_s = str(row.get("Activity.Name", "")).strip().lower()
                    crop_s = str(row.get("Crop.Name", "")).strip().lower()
                    
                    # --- NORMALIZED FINGERPRINT ---
                    try:
                        net_amount_val_s = float(row.get("Net amount", 0.0))
                        net_amount_s = f"{net_amount_val_s:.2f}"
                    except (ValueError, TypeError):
                        net_amount_s = "0.00"
                    
                    fingerprint_s = f"{doc_no_s}|{location_s}|{activity_s}|{crop_s}|{net_amount_s}"
                    if fingerprint_s in historical_fingerprints:
                        continue
                        
                    account_code = str(row.get("Account2.Code", "")).strip()
                    sub_ledger_code = str(row.get("Sub Ledger.Code", "")).strip()
                    if f"{account_code}_{sub_ledger_code}" in immunity_list:
                        continue

                    sub_dept = str(row.get('Sub Department.Name', '')).strip()
                    if not sub_dept: continue
                    
                    for (rule_sub_dept, rule_col), rule_vals_lower in rules_dict.items():
                        if sub_dept == rule_sub_dept:
                            row_val_lower = str(row.get(rule_col, '')).strip().lower()
                            log_entry = f"Row for **{user}**: Checking Sub-Dept `'{sub_dept}'`. Comparing value `'{row_val_lower}'` in column `'{rule_col}'` against rule `'{rule_vals_lower}'`."
                            
                            if row_val_lower in rule_vals_lower:
                                db_manager.log_suspicious_transaction(current_run_id, row.to_dict(), user)
                                flagged_count += 1
                                processing_log.append(log_entry + " -> **MATCH FOUND**")
                                break
                            else:
                                processing_log.append(log_entry + " -> No Match")
        
        if flagged_count > 0:
            st.success(f"✅ Flagged **{flagged_count}** new suspicious transaction(s) for manual admin review.")
        else:
            st.info("ℹ️ No transactions matched the custom suspicious rules.")
            
        with st.expander("🔍 View Suspicious Rule Check Log"):
            if not processing_log:
                st.write("No applicable rules were found for the sub-departments in this file.")
            else:
                for entry in processing_log:
                    st.markdown(entry, unsafe_allow_html=True)
        
        # --- Existing DataValidator Logic ---
        summary_tab, exceptions_tab, data_tab = st.tabs(["📊 Validation Summary", "📋 Exception Records", "📖 Processed Data"])
        
        exceptions_df_from_validation = all_exceptions_df
        # Re-calculate department statistics on the final de-duplicated dataframe
        _, department_statistics = validator.validate_dataframe(final_df_to_process)

        if not exceptions_df_from_validation.empty:
            db_manager.save_exceptions(current_run_id, exceptions_df_from_validation)
        
        db_manager.save_user_performance(current_run_id, final_df_to_process, exceptions_df_from_validation)
        if department_statistics:
            db_manager.save_department_summary(current_run_id, department_statistics)

        # --- Ghost User Detection (from your code, on de-duplicated data) ---
        try:
            all_users_in_db_df = db_manager.get_all_users()
            known_users = set(all_users_in_db_df['username'].str.lower()) if not all_users_in_db_df.empty else set()
            uploaded_users = set(final_df_to_process['Created user'].dropna().astype(str).str.lower())
            ghost_users = uploaded_users - known_users
            if ghost_users:
                ghost_users_str = ", ".join(sorted(list(ghost_users)))
                with summary_tab:
                     st.warning(f"👻 **Ghost Users Found:** The following users from the file do not exist in the system: `{ghost_users_str}`.")
                super_users = db_manager.get_users_by_role('Super User')
                if super_users:
                    message = f"In file '{uploaded_file.name}', these usernames were found but do not exist: **{ghost_users_str}**. Please add them if they are valid users."
                    for su in super_users:
                        db_manager.create_notification(username=su, notif_type='Ghost User Detected', message=message)
        except Exception as e_ghost:
            logging.error(f"Error during ghost user detection: {e_ghost}", exc_info=True)

        # --- Create and Save Excel Report (from your code) ---
        excel_report_data = create_excel_report(exceptions_df_from_validation, department_statistics, uploaded_file.name)
        if excel_report_data:
            db_manager.save_excel_report(current_run_id, excel_report_data)
            excel_report_data.seek(0)

        # --- Display Results in UI (from your code, using new counts) ---
        with summary_tab:
            st.markdown("#### 📊 File Information (Post-Deduplication)")
            col_info1, col_info2, col_info3 = st.columns(3)
            display_metric("Unique Records Processed", f"{len(final_df_to_process):,}", container=col_info1)
            display_metric("Total Columns", len(final_df_to_process.columns), container=col_info2)
            display_metric("File Size", f"{uploaded_file.size / 1024:.1f} KB", container=col_info3)

            st.markdown("#### 🛠 Standard Validation Results")
            if exceptions_df_from_validation.empty:
                st.success(f'**Perfect!** No standard validation issues found in "{uploaded_file.name}"!')
            else:
                st.warning(f'**Warning!** Found {len(exceptions_df_from_validation)} records with standard validation issues.')
                col_res1, col_res2, col_res3 = st.columns(3)
                display_metric("Total Exceptions", f"{len(exceptions_df_from_validation):,}", container=col_res1)
                current_exception_rate = (len(exceptions_df_from_validation)/len(final_df_to_process)*100) if len(final_df_to_process) > 0 else 0
                display_metric("Exception Rate", f"{current_exception_rate:.2f}%", container=col_res2)
                avg_sev = exceptions_df_from_validation['Severity'].mean() if 'Severity' in exceptions_df_from_validation.columns else 0.0
                display_metric("Average Severity", f"{avg_sev:.2f}", container=col_res3)

        with exceptions_tab:
            if exceptions_df_from_validation.empty:
                 st.success("No standard exceptions to display.")
            else:
                st.markdown("##### 📋 Standard Exception Records")
                display_interactive_exceptions(exceptions_df_from_validation, key_prefix="upload_view")
                if excel_report_data:
                    st.download_button(
                        label=f"📥 Download Standard Validation Report",
                        data=excel_report_data,
                        file_name=f"Validation_Report_{uploaded_file.name}",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
                    )
        
        with data_tab:
            st.markdown(f"#### 📖 Final De-duplicated Dataset for Processing")
            st.dataframe(final_df_to_process, use_container_width=True)

        if not final_df_to_process.empty:
            with st.spinner("Saving transaction history for future duplicate checks..."):
                processed_fingerprints = set()
                for _, row in final_df_to_process.iterrows():
                    try:
                        doc_no = str(row.get("Document No.", "")).strip().lower()
                        location = str(row.get("Location.Name", "")).strip().lower()
                        activity = str(row.get("Activity.Name", "")).strip().lower()
                        crop = str(row.get("Crop.Name", "")).strip().lower()
                        net_amount_val = float(row.get("Net amount", 0.0))
                        net_amount = f"{net_amount_val:.2f}"
                        fingerprint = f"{doc_no}|{location}|{activity}|{crop}|{net_amount}"
                        processed_fingerprints.add(fingerprint)
                    except (ValueError, TypeError):
                        continue
                db_manager.save_transaction_fingerprints(current_run_id, list(processed_fingerprints))
                st.success("Transaction history saved.")

    except Exception as e_process:
        st.error(f'An unhandled error occurred while processing "{uploaded_file.name}": {str(e_process)}')
        logging.exception(f"Unhandled error processing uploaded file {uploaded_file.name}: {e_process}")