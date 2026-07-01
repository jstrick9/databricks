# Databricks notebook source
from pyspark.sql.functions import *
import pandas as pd
from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Image, Spacer
from reportlab.lib.styles import getSampleStyleSheet

# COMMAND ----------

dbutils.widgets.text("dbx_env", "dev")
dbx_env = dbutils.widgets.get("dbx_env")

config_file_name = "trmreports-conf.yaml"
config_file = "../../config/" + dbutils.widgets.get("dbx_env") + "/" + config_file_name

print(f"{config_file=},{dbx_env=}")

# COMMAND ----------

# MAGIC %run ./../shared/ntb_common_func_and_params

# COMMAND ----------

common_configs = read_yaml(config_file)
reporting_catalog = common_configs["schema"]["trgt_catalog"]
tmngpdb_catalog = common_configs["schema"]["tmngpdb_src_catalog"]
tmngidmp_catalog = common_configs["schema"]["tmngidmp_catalog"]
spark.conf.set("config.tmngpdb_catalog", tmngpdb_catalog)
trm_scope = common_configs["secrets"]["trm_scope"]
dq_catalog = common_configs['schema']['data_quality_catalog']
altrx_schema = common_configs['schema']['altrx_schema']
primary_email = common_configs["alerting"]["efy_ap116_renewal_report"]["email"]
print(reporting_catalog, tmngpdb_catalog, tmngidmp_catalog, trm_scope, dq_catalog, altrx_schema, primary_email)

# COMMAND ----------

# set current time for job control
curntdt = datetime.datetime.now().astimezone(pytz.timezone('US/Eastern'))

# start job control  
job_start_ts = curntdt.strftime('%Y-%m-%d %H:%M:%S')
job_name = "ntb_trmreports_efy_ap116_renewal_report"
control_dt = begin_job_cntl(f"{reporting_catalog}.silver", job_name, job_start_ts)

# COMMAND ----------

# Get the current year and month
current_year = datetime.datetime.now().year
current_month = datetime.datetime.now().month
number_of_years = 10

# Calculate dynamic year
renewal_year_start = current_year + 1 if current_month >= 10 else current_year - number_of_years
renewal_year_end = renewal_year_start - number_of_years

print(current_year, current_month, renewal_year_start, renewal_year_end)


# COMMAND ----------

df = spark.sql(f"""
        WITH AP116 as (
            SELECT 
                substr(AM_DT_REG, 1, 4) as registration_year,
                CASE WHEN AM_DT_REG > 0 THEN 1 ELSE 0 END as REG,
                CASE WHEN AM_DT_RNWL > 0 THEN 1 ELSE 0 END as RNWL
            FROM {tmngpdb_catalog}.bronze.tram_am
            WHERE substr(AM_DT_REG, 1, 4) > {renewal_year_end}
            AND substr(AM_DT_REG, 1, 4) <= {renewal_year_start}
        ) 
        SELECT 
            registration_year AS Year,
            format_number(sum(REG), 0) as `Applications Registered Count`,
            format_number(sum(RNWL), 0) as `Registrations with Renewal Dates Count`,
            round((sum(RNWL) / sum(REG)) * 100, 2) as `Percent Renewed`,
            round(((sum(REG) - sum(RNWL)) / sum(REG)) * 100, 2) as `Percent Expired`
        FROM AP116
        GROUP BY registration_year
        ORDER BY registration_year
        """)

df_totals = spark.sql(f"""
        WITH AP116 as (
            SELECT 
                substr(AM_DT_REG, 1, 4) as registration_year,
                CASE WHEN AM_DT_REG > 0 THEN 1 ELSE 0 END as REG,
                CASE WHEN AM_DT_RNWL > 0 THEN 1 ELSE 0 END as RNWL
            FROM {tmngpdb_catalog}.bronze.tram_am
            WHERE substr(AM_DT_REG, 1, 4) > {renewal_year_end}
            AND substr(AM_DT_REG, 1, 4) <= {renewal_year_start}
        ) 
        SELECT 
            Count(DISTINCT registration_year) AS `Years Count`,
            format_number(sum(REG), 0) as `Total Applications Registered Count`,
            format_number(sum(RNWL), 0) as `Total Registrations with Renewal Dates Count`,
            round((sum(RNWL) / sum(REG)) * 100, 2) as `Total Percent Renewed`,
            round(((sum(REG) - sum(RNWL)) / sum(REG)) * 100, 2) as `Total Percent Expired`
        FROM AP116
        """)

# COMMAND ----------

def generate_pdf_with_one_table_fit_to_page(df, df_totals, pdf_output_path, image_left_path, image_right_path):
    # Page dimensions
    page_width, page_height = letter
    margin = 0.5 * inch  # Margin for the page
    header_margin = 0 * inch
    available_width = page_width - 1 * margin
    available_height = page_height - 3 * margin

    # Define height of header
    header_height = 2 * inch
    # Subtract header height to get available height
    available_height -= header_height 

    # Create the PDF document
    pdf = SimpleDocTemplate(
        pdf_output_path, pagesize=letter,
        leftMargin=margin, rightMargin=margin, topMargin=margin, bottomMargin=margin
    )

    # Helper function to convert DataFrame to a list of lists
    def dataframe_to_table_data(df):
        table_data = [df.columns.tolist()]  # Add headers
        table_data.extend(df.values.tolist())  # Add data rows
        return table_data

    # Convert dataframes to table data
    table_data = dataframe_to_table_data(df)
    table_data_totals = dataframe_to_table_data(df_totals)

    # Calculate proportional row heights and column widths
    total_rows = len(table_data) + len(table_data_totals)
    total_columns = len(table_data[0]) + len(table_data_totals[0])

    # Adjust row height to fit within available height
    row_height = available_height / total_rows

    # Set column widths based on the pandas dataframe
    column_widths = []
    for col in df_totals.columns:
        max_length = df_totals[col].astype(str).map(len).max()
        col_width = (max_length / df_totals.columns.map(len).max()) * available_width * 1.5
        column_widths.append(col_width)

    # Function to wrap header text
    def wrap_header_text(headers, col_widths):
        wrapped_headers = []
        for header, col_width in zip(headers, col_widths):
            wrapped_header = Paragraph(f'<para alignment="center"><font color="white">{header}</font></para>', getSampleStyleSheet()['BodyText'])
            wrapped_headers.append(wrapped_header)
        return wrapped_headers

    # Wrap header text
    table_data[0] = wrap_header_text(table_data[0], column_widths)
    table_data_totals[0] = wrap_header_text(table_data_totals[0], column_widths)

    # Function to add alternating row colors
    def add_alternating_row_colors(table, num_rows):
        page_style = TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.navy),  # Header background
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.white),  # Header text color
            ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 4),
            ('FONTSIZE', (0, 0), (-1, -1), 8),  # Small font for fitting
            ('GRID', (0, 0), (-1, -1), 0.5, colors.black)
        ])
        for row in range(1, num_rows):  # Skip header row
            bg_color = colors.white if row % 2 == 1 else colors.lightgrey
            page_style.add('BACKGROUND', (0, row), (-1, row), bg_color)
        return page_style

    # Create the totals table
    table_totals = Table(table_data_totals, colWidths=column_widths, rowHeights=row_height)
    table_totals.setStyle(add_alternating_row_colors(table_totals, len(table_data_totals)))

    # Create detail table
    table = Table(table_data, colWidths=column_widths, rowHeights=row_height)
    table.setStyle(add_alternating_row_colors(table, len(table_data)))

    # Create title
    styles = getSampleStyleSheet()
    title_style = styles['Title']
    title_style.alignment = 1  # Center alignment
    title = Paragraph("EFY AP116 RENEWAL REPORT", title_style)

    # Add images (adjust paths for shared repository)
    image_left = Image(f"{image_left_path}", width=1 * inch, height=.35 * inch)
    image_right = Image(f"{image_right_path}", width=.75 * inch, height=1 * inch)

    # Build header with title and images
    header_table = Table(
        [[image_left, title, image_right]],
        colWidths=[1 * inch, available_width - 2 * inch, 1 * inch]
    )
    header_table.setStyle(TableStyle([
        ('ALIGN', (0, 0), (0, 0), 'LEFT'),
        ('ALIGN', (1, 0), (1, 0), 'CENTER'),
        ('ALIGN', (2, 0), (2, 0), 'RIGHT'),
        ('BOTTOMPADDING', (0, 0), (0, 0), 1),
        ('TOPPADDING', (0, 0), (0, 0), 1),
    ]))

    # Add elements to the PDF
    elements = [
        Spacer(0, header_margin),  # Top margin
        header_table,
        Spacer(1, 0.1 * inch),  # Spacer between header and tables
        table_totals,
        Spacer(1, 0.1 * inch),  # Spacer between header and tables
        table
    ]

    # Build the PDF
    pdf.build(elements)
    print(f"PDF generated at {pdf_output_path}")

report_name = "EFY AP116 Renewal Report"

# Convert DataFrame to Pandas for easy iteration over rows for PDF
pandas_df = df.toPandas()
pandas_df_totals = df_totals.toPandas()
pdf_output_path = f"/dbfs/mnt/eds/trademark/dbx_reports/efy_ap116_renewal/{report_name}.pdf"
image_left_path = "../shared/uspto_logo.png"
image_right_path = "../shared/tm_analytics.jpg"

# Generate PDF
generate_pdf_with_one_table_fit_to_page(pandas_df, pandas_df_totals, pdf_output_path, image_left_path, image_right_path)

# COMMAND ----------

# Email credentials
to = primary_email
from_addr = "trademark_analytics@uspto.gov"
subj = report_name
html = f"""
        Please find attached the EFY AP116 Renewal Report in PDF format.<br><br>
        Best Regards,<br><br>
        Trademark DnA Team
        """

# Attach the PDF file
attachments = [pdf_output_path]  

# Send the email with the attachment
send_email_report(
    job_nm = job_name,
    subject = subj,
    send_from = from_addr,
    send_to = to,
    html_body= html,
    attachments = attachments
)

# COMMAND ----------

# data quality entry
df = df.select([col(c).alias(c.replace(" ", "_")) for c in df.columns])
df.write.mode("overwrite").option("mergeSchema", "true").format("delta").saveAsTable(f"{reporting_catalog}.gold.efy_ap116_renewal_report")

# COMMAND ----------

#tbl1 = f"hive_metastore.{altrx_schema}.milestone" 
#tbl2 = f"{reporting_catalog}.silver.milestone"
#key_cols = ["Renewal_status", "registration_FY"]

#dq_result = alteryx_data_match(tbl1, tbl2, key_cols, job_name, dq_catalog)
#print(dq_result)

# COMMAND ----------

end_job_cntl(
    f"{reporting_catalog}.silver",
    job_name,
    job_start_ts,
    "completed",
    0,
    "job completed successfully",
)
dbutils.notebook.exit(f"Job completed with {df.count()} records.")
