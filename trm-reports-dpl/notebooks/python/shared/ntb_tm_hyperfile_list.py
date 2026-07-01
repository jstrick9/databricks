# Databricks notebook source
# Hyperfile Column names need to match exactly, including whitespace and case sensitivity.

# COMMAND ----------

# DBTITLE 1,TRM Dashboards - List of Hyperfiles
trm_hyperfile_table_map = [
    ('TM Filings Dashboard TRM','gold','filings_dashboard'),#9901648
    ('TM Goods Services Dashboard TRM','gold','goods_services_dashboard'),#168,624,529
    ('TM Form Paragraph Dashboard TRM','gold','form_paragraph_dashboard'),#49,332,109
    ('TM PostReg Dashboard Milestone TRM','gold','post_reg_dashboard'),#6,614,710
    ('TM PostReg Dashboard Running TRM','gold','post_reg_dashboard_running'),#13,229,420
    ('TM PostReg Dashboard Detail TRM','gold','post_reg_detail_dashboard'),#10,102,274
    ('TM PostReg Workforce TRM','gold','post_reg_workforce'),#53,865
    ('TM Quality Dashboard TRM','gold','quality_dashboard'),#41393
    ('TM Quality Dashboard Pivot TRM','gold','quality_dashboard_pivot'),#579502
    ('TM Pendency Dashboard TRM','gold','pendency_dashboard'),#5,645,868
    ('TM Inventory Madrid TRM','gold','inventory_madrid'),#1
    ('TM Inventory Dashboard BD Occurrence TRM','gold','inventory_dashboard_bd_occurrence'),#12
    ('TM Inventory Dashboard EA Counts TRM','gold','inventory_dashboard_ea_counts'),#1
    ('TM Inventory Dashboard History TRM','gold','inventory_unexamined_hstry'),#table not found
    ('TM Inventory Dashboard Ratio TRM','gold','inventory_dashboard_ratio'),#1
    ('TM Inventory Dashboard Filings TRM','gold','inventory_dashboard_filings'),#2192
    ('TM Inventory Dashboard Pendency TRM','gold','inventory_dashboard_pendency'),#1
    ('TM Inventory Dashboard Running TRM','gold','inventory_dashboard_running'),#2330
    ('TM TTAB Workloads TRM','gold','ttab_workloads'),#30847
    ('TM TTAB Decision Rates TRM','gold','ttab_decision_rates'),#26391
    ('TM TTAB Dashboard TRM','gold','ttab_detail'),#0
    ]

# COMMAND ----------

# DBTITLE 1,filings_dashboard
filings_dashboard_column_map =[
    #hyperfilecolname,dbx colname
('SER_NUM','ser_num'),
('Pendency_Cal_Start_DT','pendency_cal_start_dt'),
('Filing_FY','filing_fy'),
('NON/PRO SE','non_pro_se'),
('FILING_METHOD_FILED','filing_method_filed'),
('FILING_BASIS_GRP','filing_basis_grp'),
('Class','class'),
('NAME','name'),
('CITY','city'),
('STE_CTRY_CD','ste_ctry_cd'),
('POSTAL_CD','postal_cd'),
('CTRY_NM',	'ctry_nm'),
('Country or Area Name'	,'country_or_area_name'),
('Count','count'),
('Max_Pendency_Cal_Start_DT','max_pendency_cal_start_dt'),
('Coordinated Class','coordinated_class'),
('Filing FY','filing_fy2'),
('Filing FY Month INT','filing_fy_month_int'),
('Filing FY Quarter','filing_fy_quarter'),
('Filing FY Month','filing_fy_month'),
('Top_2_Years','top_2_years'),
('Fee_Paid_Class','fee_paid_class'),
('Max_Filing_FY','max_filing_fy'),
('PCTRAM LINK','pctram_link'),
('Fixed_Count','fixed_count'),
('Realtime_Count','realtime_count'),
('TRAM_Count','tram_count'),
('Goods_or_Services','goods_or_services'),
('Concat_Goods_or_Services','concat_goods_or_services'),
('ENTITY_TYPE','entity_type'),
('Applicant_Bin','applicant_bin'),
('Output Record Count','output_record_count')
]
     

# COMMAND ----------

# DBTITLE 1,form_paragraph_dashboard
form_paragraph_dashboard_column_map = [
('Generated Date','generated_date'),
('Category','category'),
('Grade','grade'),
('Data Through Date','data_through_date'),
('Serial Number','serial_number'),
('Group Name','group_name'),
('Completed Date','completed_date'),
('Transaction Literal','transaction_literal'),
('Action Count','action_count'),
('Form Paragraph ID','form_paragraph_id'),
('Title Text','title_text'),
('Foreign Key Form Paragraph Group ID','foreign_key_form_paragraph_group_id'),
('Foreign Key Form Paragraph Category ID','foreign_key_form_paragraph_category_id'),
('Form Paragraph Year','form_paragraph_year'),
('TOC link','toc_link'),
('Concat Form Paragraph ID','concat_form_paragraph_id'),
('Concat Category','concat_category'),
('First Action Count Numerator','first_action_count_numerator'),
('First Action Count Denominator','first_action_count_denominator'),
('Filing Basis Group','filing_basis_group'),
('Exam','exam'),
('Action Type','action_type'),
('Completed Date Year','completed_date_year'),
('Completed Date Fiscal Year','completed_date_fiscal_year'),
('TM_ANALYTICS_TS','tm_analytics_ts'),
('Transaction Number','transaction_number'),
('action_type_2_possible_fix','action_type_2_possible_fix'),
('Law Office','law_office'),
('Country or Area Name','country_or_area_name'),
('STATE_CD','state_cd'	),
]

# COMMAND ----------

# DBTITLE 1,goods_services_dashboard
goods_services_dashboard_column_map = [
('SER_NUM','ser_num'),
('Class','class'),
('Coordinated Class','coordinated_class'),
('Pendency_Cal_Start_DT','pendency_cal_start_dt'),
('Filing FY','filing_fy'),
('NON/PRO SE','non_pro_se'),
('FILING_METHOD_FILED','filing_method_filed'),
('FILING_BASIS_GRP','filing_basis_grp'),
('STE_CTRY_CD','ste_ctry_cd'),
('Country or Area Name','country_or_area_name'),
('Max_Pendency_Cal_Start_DT','max_pendency_cal_start_dt'),
('Filing FY Quarter','filing_fy_quarter'),
('Filing FY Month','filing_fy_month'),
('ENTITY_TYPE','entity_type'),
('Applicant_Bin','applicant_bin'),
('Goods_or_Services','goods_or_services'),
('Goods & Services Desc','goods_services_desc'),
('Class_Count','class_count'),
]

# COMMAND ----------

# DBTITLE 1,post_reg_dashboard
post_reg_dashboard_column_map = [
('SERIAL_NUMBER','serial_number'),
('REGISTRATION_DT','registration_dt'),
('Six_YR_DT','six_yr_dt'),
('LAST_10YR_DT','last_10yr_dt'),
('Next_10Yr_Renewal','next_10yr_renewal'),
('Number_Renewals','number_renewals'),
('Next_6YR_DT','next_6yr_dt'),
('Expiration_DT','expiration_dt'),
('Expiration_TYPE','expiration_type'),
('REGISTRATION_NUMBER','registration_number'),
('AM_DT_CNCL','am_dt_cncl'),
('Live_Registration','live_registration'),
('Expiration_DT_RealTime','expiration_dt_realtime'),
('Expiration_Type_RealTime','expiration_type_realtime'),
('Live_reg','live_reg'),
('Exp_FY','exp_fy'),
('Exp_FY_RT','exp_fy_rt'),
('Reg_FY','reg_fy'),
('Today','today'),
('Today_FY','today_fy'),
('FY_Exp_Diff','fy_exp_diff'),
('FY_Reg_Diff','fy_reg_diff'),
('Six_YR_FY','six_yr_fy'),
('Ten_YR_FY','ten_yr_fy'),
('Include_6YR_AVG','include_6yr_avg'),
('Include_10YR_AVG','include_10yr_avg'),
('Max_Today_FY','max_today_fy'),
('Reg_Age','reg_age'),
('Average_Life_Include','average_life_include'),
('Sixyr_Num','sixyr_num'),
('Sixyr_Denom','sixyr_denom'),
('Tenyr_Num','tenyr_num'),
('Tenyr_Denom','tenyr_denom'),
('twentyyr_Num','twentyyr_num'),
('twentyyr_denom','twentyyr_denom'),
('thirtyyr_num','thirtyyr_num'),
('thirtyyr_denom','thirtyyr_denom'),
('fortyyr_num','fortyyr_num'),
('fortyyr_denom','fortyyr_denom'),
('fiftyyr_num','fiftyyr_num'),
('fiftyyr_denom','fiftyyr_denom'),
('Milestone','milestone'),
('Pendency_Cal_Start_DT','pendency_cal_start_dt'),
('NON/PRO SE','non_pro_se'),
('PCTRAM LINK','pctram_link'),
('LAW_OFFICE','law_office'),
('FILING_BASIS_GRP','filing_basis_grp'),
('FILING_METHOD_CUR','filing_method_cur'),
('AM_STAT','am_stat'),
('Owner_Name','owner_name'),
('CITY','city'),
('State','state'),
('Country or Area Name','country_or_area_name'),
('Reg_Class_Count','reg_class_count'),
('Active_Class_Count','active_class_count'),
('Group_Type','group_type'),
('Concat_Class','concat_class'),
('MARK_NM_SHORT','mark_nm_short'),
('Max_Dt_Filter','max_dt_filter'),
]

# COMMAND ----------

# DBTITLE 1,post_reg_dashboard_running
post_reg_dashboard_running_column_map =[
('SERIAL_NUMBER','SERIAL_NUMBER'),
('MARK_NM_SHORT','MARK_NM_SHORT'),
('Concat_Class','Concat_Class'),
('Group_Type','Group_Type'),
('Active_Class_Count','Active_Class_Count'),
('Reg_Class_Count','Reg_Class_Count'),
('Country or Area Name','Country_or_Area_Name'),
('State','State'),
('CITY','CITY'),
('Owner_Name','Owner_Name'),
('Continue_Process','Continue_Process'),
('AM_STAT','AM_STAT'),
('FILING_BASIS_GRP','FILING_BASIS_GRP'),
('LAW_OFFICE','LAW_OFFICE'),
('PCTRAM LINK','PCTRAM_LINK'),
('NON/PRO SE','NON_PRO_SE'),
('Pendency_Cal_Start_DT','Pendency_Cal_Start_DT'),
('SER_NUM','SER_NUM'),
('Max_Dt_Filter','Max_Dt_Filter'),
('LiveRegH_Count','LiveRegH_Count'),
('LiveRegH_DT','LiveRegH_DT'),
('LiveRegH_Value','LiveRegH_Value'),
('LiveRegH_Name','LiveRegH_Name'),
('FILING_METHOD_CUR','FILING_METHOD_CUR'),
]

# COMMAND ----------

# DBTITLE 1,post_reg_detail_dashboard
post_reg_detail_dashboard_column_map =[
('RecordID','recordid'),
('SERIAL_NUMBER','serial_number'),
('REGISTRATION_DT','registration_dt'),
('REGISTRATION_NUMBER','registration_number'),
('POSTREG_CATEGORY','postreg_category'),
('START_ACTION_NUMBER','start_action_number'),
('END_ACTION_NUMBER','end_action_number'),
('START_ACTION_DATE','start_action_date'),
('END_ACTION_DATE','end_action_date'),
('START_5_CHARACTERS','start_5_characters'),
('END_5_CHARACTERS','end_5_characters'),
('START_CM_DESC','start_cm_desc'),
('END_CM_DESC','end_cm_desc'),
('15_FLAG','fifteen_flag'),
('INVENTORY','inventory'),
('FIRST_ACTION_DATE','first_action_date'),
('FIRST_ACTION_CODE','first_action_code'),
('RENEWAL_DT','renewal_dt'),
('RENEWAL_NUMBER','renewal_number'),
('FIRST_ACTION_PENDENCY','first_action_pendency'),
('TOTAL_PENDENCY','total_pendency'),
('Max_Max_DT','max_max_dt'),
('Expiration_Type_RealTime2','expiration_type_realtime2'),
('Expiration_DT_RealTime2','expiration_dt_realtime2'),
('MAX_FY_PH','max_fy_ph'),
('SixYR_Disposed_Count','sixyr_disposed_count'),
('SixYR_Base','sixyr_base'),
('TenYR_Disposed_Count','tenyr_disposed_count'),
('TenYR_Base','tenyr_base'),
('End_Action_FY','end_action_fy'),
('SER_NUM','ser_num'),
('Pendency_Cal_Start_DT','pendency_cal_start_dt'),
('NON/PRO SE','non_pro_se'),
('PCTRAM LINK','pctram_link'),
('LAW_OFFICE','law_office'),
('FILING_BASIS_GRP','filing_basis_grp'),
('FILING_METHOD_CUR','filing_method_cur'),
('AM_STAT','am_stat'),
('Owner_Name','owner_name'),
('CITY','city'),
('State','state'),
('Country or Area Name','country_or_area_name'),
('Reg_Class_Count','reg_class_count'),
('Active_Class_Count','active_class_count'),
('Group_Type','group_type'),
('FA_PERCENTILE','fa_percentile'),
('Right_RecordID','right_recordid'),
('FA_PERCENTILE_INCLUDE','fa_percentile_include'),
('TP_PERCENTILE','tp_percentile'),
('TP_PERCENTILE_INCLUDE','tp_percentile_include'),
('Top10_FY_Exclude_CFY','top10_fy_exclude_cfy'),
('Top5_FY_Exclude_CFY','top5_fy_exclude_cfy'),
('RENEWAL_NUMBER_GRP','renewal_number_grp'),
('Category','category'),
('Concat_Class','concat_class'),
('FIRST_ACTION_INVENTORY','first_action_inventory'),
('REG_FY','reg_fy'),
('Drop_Off_Year','drop_off_year'),
]

# COMMAND ----------

post_reg_workforce_column_map =[
('Fiscal_Year','Fiscal_Year'),
('Date','Date'),
('PostRegCat','PostRegCat'),
('Base_Total','Base_Total'),
('Avg_6YR_Rate','Avg_6YR_Rate'),
('Avg_10YR_Rate','Avg_10YR_Rate'),
('Actual_Estimated','Actual_Estimated'),
('Continue_Process','Continue_Process'),
]

# COMMAND ----------

pendency_dashboard_column_map = [
('1st Action Pendency_PH','first_action_pendency_ph'),
('1st_Action_DT_PH','first_action_dt_ph'),
('1st_Action_Type','first_action_type_num'),
('ABANDONMENT_DT','abandonment_dt'),
('Active_Classes_Disposal','active_classes_disposal'),
('Active_Classes_FirstAction','active_classes_firstaction'),
('AM_STAT','am_stat'),
('Country or Area Name','country_or_area_name'),
('CTRY_NM','ctry_nm'),
('Disposal_DT','disposal_dt'),
('DISPOSAL_PENDENCY','disposal_pendency'),
('Disposal_Type','disposal_type'),
('FA Pendency Filter','fa_pendency_filter'),
('FA Pendency FY','fa_pendency_fy'),
('FA Pendency FY Month','fa_pendency_fy_month'),
('FA Pendency FY Quarter','fa_pendency_fy_quarter'),
('FILING_BASIS_GRP','filing_basis_grp'),
('FILING_METHOD_FILED','filing_method_filed'),
('First Action Type','first_action_type'),
('LAST_MODIFIED_DATE','last_modified_date'),
('LAW_OFFICE','law_office'),
('MAX_Action_Dt','max_action_dt'),
('NOA_DT','noa_dt'),
('NON/PRO SE','non_pro_se'),
('On_Hold','on_hold'),
('PCTRAM LINK','pctram_link'),
('Pendency_Cal_End_DT','pendency_cal_end_dt'),
('Pendency_Cal_Start_DT','pendency_cal_start_dt'),
('Pendency_Category','pendency_category'),
('POSTAL_CD','postal_cd'),
('REGISTRATION_DT','registration_dt'),
('SER_NUM','ser_num'),
('STE_CTRY_CD','ste_ctry_cd'),
('Total Pendency FY','total_pendency_fy'),
('Total Pendency FY Filter','total_pendency_fy_filter'),
('Total Pendency FY Month','total_pendency_fy_month'),
('Total Pendency FY Quarter','total_pendency_fy_quarter'),
('Total Pendency FY_Date','total_pendency_fy_date'),
('Output Record Count','output_record_count'),
]

# COMMAND ----------

quality_dashboard_pivot_column_map = [
('Law_Office','law_office'),
('lastreviewdatetime','lastreviewdatetime'),
('GO_FINAL','go_final'),
('Review_Type','review_type'),
('Final_Compliance','final_compliance'),
('qualitymetricdeficientflag','qualitymetricdeficientflag'),
('excellentflag','excellentflag'),
('MAX_Date','max_date'),
('FY_Date_Current','fy_date_current'),
('Current_FY','current_fy'),
('Current_FY_INT','current_fy_int'),
('FY_Date','fy_date'),
('FY_Date_String','fy_date_string'),
('FY_Month','fy_month'),
('FY_Month_INT','fy_month_int'),
('FY_Quarter','fy_quarter'),
('1st_Action_Type','first_action_type'),
('Disposal_Type','disposal_type'),
('Pendency_Cal_Start_DT','pendency_cal_start_dt'),
('NON/PRO SE','non_pro_se'),
('Pendency_Cal_End_DT','pendency_cal_end_dt'),
('Country or Area Name','country_or_area_name'),
('FILING_BASIS_GRP','filing_basis_grp'),
('FILING_METHOD_FILED','filing_method_filed'),
('STE_CTRY_CD','ste_ctry_cd'),
('Concat_Class','concat_class'),
('Metric','metric'),
('Value','value'),
('Case Count','case_count'),
('Category','category'),
]


# COMMAND ----------

quality_dashboard_column_map = [
('Law_Office','law_office'),
('lastreviewdatetime','lastreviewdatetime'),
('searchsufficientindicator','searchsufficientindicator'),
('qualitymetricdeficientindicator','qualitymetricdeficientindicator'),
('mississueindicator','mississueindicator'),
('newissueindicator','newissueindicator'),
('refusalunsoundindicator','refusalunsoundindicator'),
('substantivedeficientindicator','substantivedeficientindicator'),
('proceduraldeficientindicator','proceduraldeficientindicator'),
('overalldeficientindicator','overalldeficientindicator'),
('overallexcellentindicator','overallexcellentindicator'),
('evidencedeficientindicator','evidencedeficientindicator'),
('evidencesatisfactoryindicator','evidencesatisfactoryindicator'),
('evidenceexcellentindicator','evidenceexcellentindicator'),
('writingdeficientindicator','writingdeficientindicator'),
('writingsatisfactoryindicator','writingsatisfactoryindicator'),
('writingexcellentindicator','writingexcellentindicator'),
('substantiveerrorindicator','substantiveerrorindicator'),
('satisfactoryindicator','satisfactoryindicator'),
('findingindicator','findingindicator'),
('GO_FINAL','go_final'),
('quality_review_id','quality_review_id'),
('Review_Type','review_type'),
('Final_Compliance','final_compliance'),
('qualitymetricdeficientflag','qualitymetricdeficientflag'),
('excellentflag','excellentflag'),
('MAX_Date','max_date'),
('FY_Date_Current','fy_date_current'),
('Current_FY','current_fy'),
('Current_FY_INT','current_fy_int'),
('FY_Date','fy_date'),
('FY_Date_String','fy_date_string'),
('FY_Month','fy_month'),
('FY_Month_INT','fy_month_int'),
('FY_Quarter','fy_quarter'),
('1st_Action_Type','first_action_type'),
('Disposal_Type','disposal_type'),
('Pendency_Cal_Start_DT','pendency_cal_start_dt'),
('Pendency_Cal_End_DT','pendency_cal_end_dt'),
('NON/PRO SE','non_pro_se'),
('Country or Area Name','country_or_area_name'),
('FILING_BASIS_GRP','filing_basis_grp'),
('FILING_METHOD_FILED','filing_method_filed'),
('STE_CTRY_CD','ste_ctry_cd'),
('Concat_Class','concat_class'),
]

# COMMAND ----------

inventory_madrid_column_map = [
('MADRID_PCT'	,	'MADRID_PCT'),
('MADRID_FA_Pendency'	,	'MADRID_FA_Pendency'),
]

# COMMAND ----------

inventory_dashboard_bd_occurrence_column_map = [
('FA_Month','FA_Month'),
('Percent_of_FAs','Percent_of_FAs'),
]

# COMMAND ----------

inventory_dashboard_ea_counts_column_map = [
('EA_Not_Exam','EA_Not_Exam'),
('EA_Examining','EA_Examining'),
]

# COMMAND ----------

inventory_unexamined_hstry_column_map = [
('Unexamined_Date'	,	'unexamined_date'),
('Unexamined_Cases'	,	'unexamined_cases'),
('Unexamined_Classes'	,	'unexamined_classes'),
('FY'	,	'fy'),
('EA_Examining'	,	'ea_examining'),
('EA_Unexamined_Ratio'	,	'ea_unexamined_ratio'),
('Current_FY'	,	'current_fy'),
]

# COMMAND ----------

inventory_dashboard_ratio_column_map = [
('FY'	,	'FY'),
('EA_Examining'	,	'EA_Examining'),
('Unexamined_Classes'	,	'Unexamined_Classes'),
('EA_Unexamined_Ratio'	,	'EA_Unexamined_Ratio'),
('Current_FY'	,	'Current_FY'),
]

# COMMAND ----------

inventory_dashboard_filings_column_map = [
('Pendency_Cal_Start_DT'	,	'Pendency_Cal_Start_DT'),
('Class Count'	,	'Class_Count'),
('Count_Type'	,	'Count_Type'),
('Current_FY'	,	'Current_FY'),
('FY'	,	'FY'),
('FY_Plus'	,	'FY_Plus'),
('CurrentFY_CountType'	,	'CurrentFY_CountType'),
]

# COMMAND ----------

inventory_dashboard_pendency_column_map = [
('Sum_FAPendencyWeight'	,	'Sum_FAPendencyWeight'),
('Sum_Active_Classes_FirstAction'	,	'Sum_Active_Classes_FirstAction'),
('Current FY Weighted First Action Pendency'	,	'Current_FY_Weighted_First_Action_Pendency'),
('Data Through',	'Data_Through'),
]

# COMMAND ----------

inventory_dashboard_running_column_map =[
('Pendency_Cal_Start_DT','Pendency_Cal_Start_DT'),
('Class Count','Class_Count'),
('Count_Type','Count_Type'),
('Current_FY','Current_FY'),
('FY','FY'),
('FY_Plus','FY_Plus'),
('Start_Non_Outlier','Start_Non_Outlier'),
('RunTot_Class Count','RunTot_Class_Count'),
('EA_Not_Exam','EA_Not_Exam'),	
('EA_Examining','EA_Examining'),
('Today_Unexamined','Today_Unexamined'),
('CurrentFY_CountType','CurrentFY_CountType'),
]

# COMMAND ----------

ttab_workloads_column_map = [
('Fiscal_Year','fiscal_year'),
('Date','date'),
('TTAB_Case_Type','ttab_case_type'),
('Day_Total','day_total'),
('Actual_Estimated','actual_estimated'),
('FY_Base_Total','fy_base_total'),
('FY_Judge_Decisions','fy_judge_decisions'),
('FY_JDR','fy_jdr'),
('Latest_5YR_AVG_JDR','latest_5yr_avg_jdr'),
('Raw_Credits','raw_credits'),
('Credits_JDR_Applied','credits_jdr_applied')
]

# COMMAND ----------

ttab_decision_rates_column_map = [
('FISCAL_YEAR','fiscal_year'),
('CASE_END_DT','case_end_dt'),
('TTAB_CASE_TYPE','ttab_case_type'),
('TOTAL_DECISIONS','total_decisions'),
('TOTAL_JUDGE_DECISIONS','total_judge_decisions'),
]

# COMMAND ----------

ttab_detail_column_map = [
('SERIAL_NUMBER'	,	'serial_number'),
('TTAB_ISSUE_TYPE'	,	'ttab_issue_type'),
('PROCEEDING_NUM'	,	'proceeding_num'),
('FILING_DATE'	,	'filing_date'),
('INSTITUTED_DATE'	,	'instituted_date'),
('INSTITUTED_CODE'	,	'instituted_code'),
('DECISION_DATE'	,	'decision_date'),
('DECISION_CODE'	,	'decision_code'),
('DECISION_DESCRIPTION'	,	'decision_description'),
('TERMINATION_CODE'	,	'termination_code'),
('TERMINATION_DATE'	,	'termination_date'),
('TERMINATION_DATE_2'	,	'termination_date_2'),
('TERMINATION_DATE_3'	,	'termination_date_3'),
('TERMINATION_DATE_4'	,	'termination_date_4'),
('TERMINATION_DATE_5'	,	'termination_date_5'),
('FINAL_REFUSAL_DATE'	,	'final_refusal_date'),
('FP_REASON_1'	,	'fp_reason_1'),
('FP_REASON_2'	,	'fp_reason_2'),
('FP_REASON_3'	,	'fp_reason_3'),
('FP_REASON_4'	,	'fp_reason_4'),
('FP_REASON_5'	,	'fp_reason_5'),
('PENDENCY_D'	,	'pendency_d'),
('PENDENCY_T'	,	'pendency_t'),
('PENDENCY_R'	,	'pendency_r'),
('INVENTORY'	,	'inventory'),
('NON/PRO SE'	,	'non_pro_se'),
('PCTRAM LINK'	,	'pctram_link'),
('LAW_OFFICE'	,	'law_office'),
('FILING_BASIS_GRP'	,	'filing_basis_grp'),
('FILING_METHOD_CUR'	,	'filing_method_cur'),
('AM_STAT'	,	'am_stat'),
('Owner_Name'	,	'owner_name'),
('CITY'	,	'city'),
('State'	,	'state'),
('Country or Area Name'	,	'country_or_area_name'),
('Reg_Class_Count'	,	'reg_class_count'),
('Active_Class_Count'	,	'active_class_count'),
('Group_Type'	,	'group_type'),
('Concat_Class'	,	'concat_class'),
('MARK_NM_SHORT'	,	'mark_nm_short'),
('REFUSAL'	,	'refusal'),
('APPEAL'	,	'appeal'),
('PUBLICATION_DATE'	,	'publication_date'),
('PUBS'	,	'pubs'),
('OPPOSITION'	,	'opposition'),
('DEFAULT_OPPOSITION'	,	'default_opposition'),
('DEFAULT_CANCELLATION'	,	'default_cancellation'),
('CANCELLATION'	,	'cancellation'),
('CONSTRUCTED_PRCD_NUM'	,	'constructed_prcd_num'),
('DEFAULT_DATE'	,	'default_date'),
('Cancellation_Count'	,	'cancellation_count'),
('REG_YR'	,	'reg_yr'),
('LIVE_REG_COUNT'	,	'live_reg_count'),
('CAN_RATE'	,	'can_rate'),
('CONCURRENT'	,	'concurrent'),
('RFD_DATE'	,	'rfd_date'),
('RFD_Valid'	,	'rfd_valid'),
('PROCEEDING_COUNT'	,	'proceeding_count'),
('Case_Age_RFD'	,	'case_age_rfd'),
('Case_Age_Category'	,	'case_age_category'),
]
