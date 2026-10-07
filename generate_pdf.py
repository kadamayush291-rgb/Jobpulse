import os
import sys
from reportlab.lib import colors
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle, HRFlowable, KeepTogether
)
from reportlab.pdfgen import canvas

class NumberedCanvas(canvas.Canvas):
    def __init__(self, *args, **kwargs):
        super(NumberedCanvas, self).__init__(*args, **kwargs)
        self._saved_page_states = []

    def showPage(self):
        self._saved_page_states.append(dict(self.__dict__))
        self._startPage()

    def save(self):
        num_pages = len(self._saved_page_states)
        for state in self._saved_page_states:
            self.__dict__.update(state)
            self.draw_page_decorations(num_pages)
            super(NumberedCanvas, self).showPage()
        super(NumberedCanvas, self).save()

    def draw_page_decorations(self, page_count):
        self.saveState()
        
        # Header (pages after page 1)
        if self._pageNumber > 1:
            self.setFont("Helvetica-Bold", 8)
            self.setFillColor(colors.HexColor("#4F46E5")) # Indigo Accent
            self.drawString(54, 750, "JOBPULSE — AUTOMATED DATA ENGINEERING PIPELINE")
            self.setFont("Helvetica", 8)
            self.setFillColor(colors.HexColor("#6B7280"))
            self.drawRightString(612 - 54, 750, "PROJECT DOCUMENTATION")
            self.setStrokeColor(colors.HexColor("#E5E7EB"))
            self.setLineWidth(0.75)
            self.line(54, 742, 612 - 54, 742)
            
        # Footer (all pages)
        self.setStrokeColor(colors.HexColor("#E5E7EB"))
        self.setLineWidth(0.75)
        self.line(54, 45, 612 - 54, 45)
        
        self.setFont("Helvetica", 8)
        self.setFillColor(colors.HexColor("#6B7280"))
        self.drawString(54, 32, "JobPulse DE Project | Apache Airflow ETL Pipeline")
        
        page_text = f"Page {self._pageNumber} of {page_count}"
        self.drawRightString(612 - 54, 32, page_text)
        
        self.restoreState()

def build_pdf(filename):
    doc = SimpleDocTemplate(
        filename,
        pagesize=letter,
        leftMargin=54,
        rightMargin=54,
        topMargin=54,
        bottomMargin=54
    )
    
    styles = getSampleStyleSheet()
    
    # Custom styles
    primary_color = colors.HexColor("#1E1B4B")   # Deep Navy
    accent_color = colors.HexColor("#4F46E5")    # Indigo
    text_dark = colors.HexColor("#1F2937")       # Off black
    bg_light = colors.HexColor("#F9FAFB")        # Card background
    border_color = colors.HexColor("#E5E7EB")    # Soft Gray Border
    
    title_style = ParagraphStyle(
        'DocTitle',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=24,
        leading=28,
        textColor=primary_color,
        spaceAfter=6
    )
    
    subtitle_style = ParagraphStyle(
        'DocSubTitle',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=12,
        leading=16,
        textColor=accent_color,
        spaceAfter=15
    )
    
    meta_style = ParagraphStyle(
        'MetaText',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9,
        leading=13,
        textColor=colors.HexColor("#4B5563")
    )
    
    h1_style = ParagraphStyle(
        'SectionH1',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=14,
        leading=18,
        textColor=primary_color,
        spaceBefore=14,
        spaceAfter=8,
        keepWithNext=True
    )

    h2_style = ParagraphStyle(
        'SectionH2',
        parent=styles['Normal'],
        fontName='Helvetica-Bold',
        fontSize=11,
        leading=15,
        textColor=accent_color,
        spaceBefore=10,
        spaceAfter=4,
        keepWithNext=True
    )
    
    body_style = ParagraphStyle(
        'BodyDark',
        parent=styles['Normal'],
        fontName='Helvetica',
        fontSize=9.5,
        leading=14,
        textColor=text_dark,
        spaceAfter=6
    )

    bullet_style = ParagraphStyle(
        'BulletText',
        parent=body_style,
        leftIndent=12,
        spaceAfter=4
    )

    code_style = ParagraphStyle(
        'CodeStyle',
        parent=styles['Normal'],
        fontName='Courier',
        fontSize=8.5,
        leading=11,
        textColor=colors.HexColor("#0F172A"),
        spaceAfter=4
    )

    callout_style = ParagraphStyle(
        'CalloutText',
        parent=styles['Normal'],
        fontName='Helvetica-Oblique',
        fontSize=9,
        leading=13,
        textColor=colors.HexColor("#374151")
    )

    story = []
    
    # Header Banner
    story.append(Paragraph("JobPulse — Data Engineering Project Documentation", title_style))
    story.append(Paragraph("Automated ETL Pipeline Architecture, Apache Airflow DAG, and Analytics Dashboard", subtitle_style))
    
    meta_table_data = [
        [
            Paragraph("<b>Project Domain:</b> Data Engineering / ETL Workflow", meta_style),
            Paragraph("<b>Orchestrator:</b> Apache Airflow (Dockerized)", meta_style)
        ],
        [
            Paragraph("<b>Database:</b> PostgreSQL 15", meta_style),
            Paragraph("<b>Frontend & API:</b> Flask + Modern Animated Dashboard", meta_style)
        ]
    ]
    meta_table = Table(meta_table_data, colWidths=[250, 254])
    meta_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), bg_light),
        ('BOX', (0,0), (-1,-1), 1, border_color),
        ('INNERGRID', (0,0), (-1,-1), 0.5, border_color),
        ('TOPPADDING', (0,0), (-1,-1), 6),
        ('BOTTOMPADDING', (0,0), (-1,-1), 6),
        ('LEFTPADDING', (0,0), (-1,-1), 10),
        ('RIGHTPADDING', (0,0), (-1,-1), 10),
    ]))
    story.append(meta_table)
    story.append(Spacer(1, 14))
    
    # Section Divider helper
    def make_section_header(title):
        p = Paragraph(title.upper(), h1_style)
        hr = HRFlowable(width="100%", thickness=1.5, color=accent_color, spaceBefore=2, spaceAfter=8)
        return [p, hr]

    # ==================== 1. AIM ====================
    story.extend(make_section_header("1. Aim"))
    aim_p1 = Paragraph(
        "The primary objective of the <b>JobPulse Data Engineering Project</b> is to design, develop, and deploy an automated, end-to-end Data Engineering <b>ETL (Extract, Transform, Load) Pipeline</b> orchestrated by <b>Apache Airflow</b>.",
        body_style
    )
    aim_p2 = Paragraph("Key goals include:", body_style)
    aim_b1 = Paragraph("• <b>Automated Data Ingestion:</b> Extract raw job market datasets from CSV files or live user uploads via an API endpoint.", bullet_style)
    aim_b2 = Paragraph("• <b>Automated Workflow Orchestration:</b> Schedule and execute tasks in precise order using Apache Airflow Directed Acyclic Graphs (DAGs).", bullet_style)
    aim_b3 = Paragraph("• <b>Data Transformation & Cleaning:</b> Standardize job titles, clean messy salary ranges, extract technology skills, and calculate salary midpoints using Python and Pandas.", bullet_style)
    aim_b4 = Paragraph("• <b>Data Quality Validation:</b> Enforce schema validation rules (e.g., non-null checks, realistic numeric ranges) post-transformation before loading to database.", bullet_style)
    aim_b5 = Paragraph("• <b>Relational Storage:</b> Persist raw data, cleaned records, skill relationships, aggregated analytics, and execution logs into PostgreSQL tables.", bullet_style)
    aim_b6 = Paragraph("• <b>Interactive SaaS Web Dashboard:</b> Provide a sleek, animated, user-friendly interface for uploading raw datasets, executing the ETL pipeline in real-time, and viewing analytical charts.", bullet_style)
    
    story.extend([aim_p1, aim_p2, aim_b1, aim_b2, aim_b3, aim_b4, aim_b5, aim_b6, Spacer(1, 10)])

    # ==================== 2. THEORY ====================
    story.extend(make_section_header("2. Theory"))
    
    theory_p1 = Paragraph(
        "Modern enterprise data architectures rely on robust <b>Data Engineering Pipelines</b> to turn raw, unstructured, or semi-structured data into high-value analytics. Below are the key foundational concepts utilized in JobPulse:",
        body_style
    )
    
    story.append(theory_p1)
    
    story.append(Paragraph("<b>A. ETL Architecture (Extract, Transform, Load)</b>", h2_style))
    story.append(Paragraph("ETL is the core standard in data engineering. <i>Extraction</i> retrieves data from source systems. <i>Transformation</i> cleanses, normalizes, and enriches raw records. <i>Loading</i> writes formatted data into target data stores (data warehouses or operational databases) for analytical querying.", body_style))

    story.append(Paragraph("<b>B. Workflow Orchestration with Apache Airflow</b>", h2_style))
    story.append(Paragraph("Workflow orchestration coordinates complex multi-step data pipelines. <b>Apache Airflow</b> is an open-source platform programmatically configured using Python. It ensures tasks execute sequentially based on dependencies, handles retries on failures, logs task outcomes, and monitors execution status through a web console.", body_style))

    story.append(Paragraph("<b>C. Directed Acyclic Graph (DAG)</b>", h2_style))
    story.append(Paragraph("A <b>DAG</b> is a collection of all tasks you want to run, organized in a way that reflects their relationships and dependencies. It is <b>Directed</b> because tasks execute in a specific direction (A → B → C), and <b>Acyclic</b> because loops are prohibited, preventing infinite execution deadlocks.", body_style))

    story.append(Paragraph("<b>D. Containerization via Docker & Docker Compose</b>", h2_style))
    story.append(Paragraph("Docker packages applications and dependencies into isolated containers. Docker Compose coordinates multi-container services (Airflow Webserver, Airflow Scheduler, PostgreSQL Database, and Dashboard Application) ensuring identical environment configuration across development and production.", body_style))

    story.append(Spacer(1, 10))

    # ==================== 3. EXPLANATION ====================
    story.extend(make_section_header("3. Explanation (Tools & Components Breakdown)"))

    expl_intro = Paragraph("Each component in the JobPulse architecture is selected for a specific data engineering role:", body_style)
    story.append(expl_intro)

    table_data = [
        [Paragraph("<b>Technology / Tool</b>", meta_style), Paragraph("<b>Role / Purpose in JobPulse</b>", meta_style), Paragraph("<b>Implementation Detail</b>", meta_style)],
        
        [Paragraph("<b>Apache Airflow</b>", meta_style), 
         Paragraph("Pipeline Orchestrator & Task Scheduler", meta_style), 
         Paragraph("Schedules daily DAG runs, manages task dependencies, triggers Python scripts via <code>PythonOperator</code>, handles retries & logging.", meta_style)],

        [Paragraph("<b>Python & Pandas</b>", meta_style), 
         Paragraph("Data Manipulation & Transformation Engine", meta_style), 
         Paragraph("Reads raw CSV data, performs text cleanup, standardizes job titles, computes salary statistics, parses skill keywords, and exports DataFrames.", meta_style)],

        [Paragraph("<b>PostgreSQL 15</b>", meta_style), 
         Paragraph("Relational Data Warehouse & Storage", meta_style), 
         Paragraph("Houses structured relational tables: <code>raw_jobs</code>, <code>transformed_jobs</code>, <code>job_skills</code>, <code>job_analytics</code>, and <code>pipeline_runs</code>.", meta_style)],

        [Paragraph("<b>Flask Web Server</b>", meta_style), 
         Paragraph("Backend Web API & Data Provider", meta_style), 
         Paragraph("Serves dashboard HTML templates, exposes REST endpoints (<code>/api/upload</code>, <code>/api/trigger-pipeline</code>, <code>/api/metrics</code>) to trigger ETL inline.", meta_style)],

        [Paragraph("<b>Modern HTML5/JS Frontend</b>", meta_style), 
         Paragraph("Interactive UI & Analytical Visualization", meta_style), 
         Paragraph("Renders live charts (Chart.js), dataset preview tables, KPI counters, and CSV file upload modal with real-time status notifications.", meta_style)],

        [Paragraph("<b>Docker & Compose</b>", meta_style), 
         Paragraph("Infrastructure Orchestration", meta_style), 
         Paragraph("Runs Airflow Postgres, Scheduler, Webserver, and Dashboard backend in isolated environment containers via <code>docker-compose.yml</code>.", meta_style)]
    ]

    t = Table(table_data, colWidths=[100, 160, 244])
    t.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), colors.HexColor("#EEF2FF")),
        ('TEXTCOLOR', (0,0), (-1,0), primary_color),
        ('GRID', (0,0), (-1,-1), 0.5, border_color),
        ('BOX', (0,0), (-1,-1), 1, border_color),
        ('TOPPADDING', (0,0), (-1,-1), 5),
        ('BOTTOMPADDING', (0,0), (-1,-1), 5),
        ('LEFTPADDING', (0,0), (-1,-1), 6),
        ('RIGHTPADDING', (0,0), (-1,-1), 6),
    ]))
    story.append(t)
    story.append(Spacer(1, 10))

    # Detailed Explanation of DAG in this Project
    story.append(Paragraph("<b>Detailed Breakdown: What is DAG & Where is it Implemented?</b>", h2_style))
    story.append(Paragraph("• <b>File Location:</b> Implemented in <code>dags/jobpulse_etl_dag.py</code> under the DAG ID <code>jobpulse_etl_pipeline</code>.", bullet_style))
    story.append(Paragraph("• <b>Purpose:</b> Defines the exact workflow structure, task execution order, execution schedule (<code>@daily</code>), default parameters (retries=1, retry_delay=5 mins), and task callables.", bullet_style))
    story.append(Paragraph("• <b>Task Ordering (Dependency Chain):</b>", bullet_style))
    
    dag_chain_box = [
        [Paragraph("<code>extract_task >> transform_task >> validate_task >> load_task >> analytics_task >> log_task</code>", code_style)]
    ]
    dag_table = Table(dag_chain_box, colWidths=[504])
    dag_table.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,-1), colors.HexColor("#F1F5F9")),
        ('BOX', (0,0), (-1,-1), 1, colors.HexColor("#CBD5E1")),
        ('TOPPADDING', (0,0), (-1,-1), 8),
        ('BOTTOMPADDING', (0,0), (-1,-1), 8),
        ('LEFTPADDING', (0,0), (-1,-1), 12),
        ('RIGHTPADDING', (0,0), (-1,-1), 12),
    ]))
    story.append(dag_table)
    story.append(Spacer(1, 10))

    # ==================== 4. WORKFLOW ====================
    story.extend(make_section_header("4. Workflow (Step-by-Step Overview)"))

    wf_intro = Paragraph("The end-to-end execution flow of JobPulse operates seamlessly from user interaction to database visualization:", body_style)
    story.append(wf_intro)

    wf_steps = [
        ("Step 1: Dataset Upload", "User accesses the web dashboard at <code>http://localhost:5000</code> and uploads a CSV file containing raw job postings (or uses the default seed dataset)."),
        ("Step 2: API Request & Trigger", "The dashboard backend endpoint <code>/api/upload</code> receives the file, saves it to <code>data/raw_jobs.csv</code>, and executes the ETL execution script inline."),
        ("Step 3: Airflow DAG Execution", "Airflow (or local runner) executes the 6 sequential tasks defined in the DAG dependency graph."),
        ("Step 4: Data Processing & Cleaning", "Raw CSV rows are parsed, salary values are extracted into numeric format, job categories are assigned, missing fields are handled, and technology skills are isolated."),
        ("Step 5: Quality Check", "Automated validation asserts data integrity (verifies schema consistency, non-negative salary midpoints, and expected record counts)."),
        ("Step 6: Storage in PostgreSQL", "Validated records are inserted into PostgreSQL target tables (<code>transformed_jobs</code>, <code>job_skills</code>, and <code>job_analytics</code>)."),
        ("Step 7: Real-Time Dashboard Update", "The frontend dashboard automatically re-queries the PostgreSQL database via Flask REST APIs and updates analytical charts and counters without requiring a page refresh.")
    ]

    for title, desc in wf_steps:
        s_title = Paragraph(f"<b>{title}</b>", h2_style)
        s_desc = Paragraph(desc, bullet_style)
        story.extend([s_title, s_desc])

    story.append(Spacer(1, 10))

    # ==================== 5. PIPELINE ====================
    story.extend(make_section_header("5. Pipeline (Detailed Task Breakdown)"))

    pipe_intro = Paragraph("The JobPulse pipeline consists of 6 sequential tasks executed by Apache Airflow Python Operators:", body_style)
    story.append(pipe_intro)

    pipe_tasks = [
        ("1. Extract Task (extract_csv_data)", "extract.py",
         "Reads the raw input file from <code>data/raw_jobs.csv</code>. Validates column headers (e.g., job_title, company, location, salary, skills). Inserts raw un-modified records directly into PostgreSQL table <code>raw_jobs</code> for auditability and lineage tracking."),
        
        ("2. Transform Task (transform_data)", "transform.py",
         "Applies Python and Pandas cleaning transformations:<br/>"
         "• Normalizes job titles into standard categories (Data Engineer, Data Scientist, ML Engineer, Data Analyst).<br/>"
         "• Cleans salary strings (e.g., '$120,000 - $150,000') into <code>salary_min</code>, <code>salary_max</code>, and calculates <code>salary_midpoint</code>.<br/>"
         "• Handles missing values with sensible defaults ('Unknown Location', 'Not Specified').<br/>"
         "• Standardizes experience levels (Entry, Mid, Senior, Lead)."),

        ("3. Validate Task (validate_raw_data)", "validate.py",
         "Performs Data Quality Assurance on transformed records post-cleaning:<br/>"
         "• Asserts total transformed record count matches raw extracted count.<br/>"
         "• Checks that mandatory fields (job_title, company) contain zero null values.<br/>"
         "• Verifies <code>salary_midpoint</code> contains positive non-negative numerical values."),

        ("4. Load Task (load_transformed_data)", "load.py",
         "Writes processed DataFrames into PostgreSQL relational database tables:<br/>"
         "• Inserts cleaned jobs into <code>transformed_jobs</code> table.<br/>"
         "• Extracts skill arrays, explodes individual skills, and populates <code>job_skills</code> mapping table for skill frequency analysis."),

        ("5. Analytics Task (compute_analytics)", "analytics.py",
         "Computes pre-aggregated business metrics and statistical rollups:<br/>"
         "• Average, min, and max salary midpoints grouped by Job Role.<br/>"
         "• Top 10 most in-demand technical skills across all job listings.<br/>"
         "• Distribution of job postings across locations and remote work types.<br/>"
         "• Stores pre-computed metric outputs into <code>job_analytics</code> table for instant dashboard retrieval."),

        ("6. Log Task (log_pipeline_run)", "jobpulse_etl_dag.py",
         "Logs execution status, total processed records count, execution timestamp, and completion state into <code>pipeline_runs</code> table for operational observability.")
    ]

    for name, script, detail in pipe_tasks:
        p_name = Paragraph(f"<b>Task: {name}</b> (Script: <code>scripts/{script}</code>)", h2_style)
        p_detail = Paragraph(detail, body_style)
        story.extend([p_name, p_detail, Spacer(1, 2)])

    story.append(Spacer(1, 10))

    # ==================== 6. CONCLUSION ====================
    story.extend(make_section_header("6. Conclusion"))

    conc_p1 = Paragraph(
        "The <b>JobPulse Data Engineering Pipeline</b> successfully demonstrates a modern, enterprise-grade ETL workflow built on solid data engineering fundamentals.",
        body_style
    )
    conc_p2 = Paragraph("<b>Key Takeaways & Achievements:</b>", body_style)
    conc_b1 = Paragraph("• <b>Reliable Orchestration:</b> Apache Airflow DAGs guarantee strict sequential execution and error handling for complex data pipelines.", bullet_style)
    conc_b2 = Paragraph("• <b>Robust Data Quality:</b> Post-transformation validation prevents bad data from corrupting downstream database tables and analytical reports.", bullet_style)
    conc_b3 = Paragraph("• <b>Scalable Storage Architecture:</b> PostgreSQL table normalization separates raw audit logs, cleaned transactional data, exploded skill relations, and aggregated analytics metrics.", bullet_style)
    conc_b4 = Paragraph("• <b>Seamless User Experience:</b> The interactive web dashboard allows users to upload custom datasets and immediately view transformed analytical insights with visually striking charts.", bullet_style)
    conc_p3 = Paragraph("This project serves as a comprehensive, end-to-end practical demonstration of Data Engineering principles suitable for academic assessment and practical industry deployment.", body_style)

    story.extend([conc_p1, conc_p2, conc_b1, conc_b2, conc_b3, conc_b4, Spacer(1, 6), conc_p3])

    doc.build(story, canvasmaker=NumberedCanvas)
    print(f"Successfully generated PDF documentation at: {filename}")

if __name__ == "__main__":
    out_pdf = os.path.join(os.path.dirname(__file__), "JobPulse_Project_Documentation.pdf")
    build_pdf(out_pdf)
    # Also copy to Desktop if accessible
    desktop_pdf = r"c:\Users\Ayush\OneDrive\Desktop\JobPulse_Project_Documentation.pdf"
    try:
        import shutil
        shutil.copyfile(out_pdf, desktop_pdf)
        print(f"Copied to Desktop at: {desktop_pdf}")
    except Exception as e:
        print(f"Note: Could not copy to desktop path directly: {e}")
