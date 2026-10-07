"""
JobPulse - Multi-Domain Streaming Dataset Generator
Generates 10 diverse, uniquely-themed job market datasets across different
industries, geographies, and roles. Each dataset has realistic variance in
schema values, salary ranges, date ranges, and skill sets.
Datasets are written to data/streaming_datasets/ and ready for the 1-minute
Kafka -> Spark -> DuckDB streaming pipeline.
"""
import os
import sys
import csv
import random
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("JobPulse.DatasetGenerator")

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
STREAMING_DIR = os.path.join(DATA_DIR, "streaming_datasets")
os.makedirs(STREAMING_DIR, exist_ok=True)

FIELDNAMES = ["job_title", "company", "location", "salary", "job_type", "experience_level", "skills", "posted_date", "source", "description"]

random.seed(42)


def _rand_date(days_back: int = 60) -> str:
    base = datetime(2024, 9, 20)
    return (base - timedelta(days=random.randint(0, days_back))).strftime("%Y-%m-%d")


def _salary(lo: int, hi: int, spread: int = 20000) -> str:
    lo = lo + random.randint(-spread // 2, spread // 2)
    hi = hi + random.randint(0, spread)
    return f"{max(lo, 30000)}-{hi}"


def _inr_salary(lo: int, hi: int) -> str:
    lo = lo + random.randint(-100000, 100000)
    hi = hi + random.randint(0, 200000)
    return f"{max(lo, 400000)}-{hi}"


def _pick(lst: List) -> Any:
    return random.choice(lst)


def _skills(pool: List[str], n: int = 5) -> str:
    return ", ".join(random.sample(pool, min(n, len(pool))))


# ---------------------------------------------------------------------------
# DATASET 5 - US FinTech & Banking Engineering
# ---------------------------------------------------------------------------
def gen_dataset_5_us_fintech(n: int = 25) -> List[Dict]:
    titles = [
        "Payments Data Engineer", "Risk Analytics Engineer", "Capital Markets Quant Developer",
        "Fraud Detection ML Engineer", "Core Banking Platform Engineer", "Compliance Data Architect",
        "Credit Scoring Data Scientist", "Treasury Systems Engineer", "Anti-Money Laundering Engineer",
        "Blockchain Data Engineer", "Regulatory Reporting Analyst", "Algorithmic Trading Engineer",
        "Trade Surveillance Developer", "Financial Data Mesh Architect", "Open Banking API Engineer"
    ]
    companies = ["JPMorgan Chase", "Goldman Sachs", "Stripe", "Plaid", "Brex", "Chime", "Robinhood",
                 "Coinbase", "Block (Square)", "Affirm", "Capital One", "Visa", "Mastercard", "Klarna", "Adyen"]
    locations = ["New York, NY", "San Francisco, CA", "Charlotte, NC", "Chicago, IL", "Boston, MA",
                 "Atlanta, GA", "Austin, TX", "Seattle, WA", "Dallas, TX", "Jersey City, NJ"]
    levels = ["Entry", "Mid", "Senior", "Lead", "Principal"]
    skills_pool = ["Python", "Scala", "Kafka", "Spark", "Flink", "SQL", "dbt", "Snowflake", "Databricks",
                   "Airflow", "AWS", "GCP", "Azure", "Redis", "PostgreSQL", "ClickHouse", "Monte Carlo",
                   "Great Expectations", "Terraform", "Docker", "Kubernetes", "FIX Protocol", "SWIFT"]
    sources = ["LinkedIn", "Indeed", "Glassdoor", "Dice", "eFinancialCareers"]
    descs = [
        "Build real-time fraud signal pipelines processing 2M+ transactions per day",
        "Design regulatory capital and liquidity reporting data infrastructure",
        "Architect distributed ledger analytics for settlement and reconciliation",
        "Develop AML transaction monitoring graph pipelines using Neo4j and Spark",
        "Optimize credit risk scoring models with feature stores and MLflow",
        "Create end-to-end payments observability platform with OpenTelemetry and Grafana",
        "Implement DORA-compliant operational resilience data reporting systems",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Entry": 90000, "Mid": 125000, "Senior": 160000, "Lead": 195000, "Principal": 230000}[lvl]
        base_hi = base_lo + 55000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi), "job_type": _pick(["Full-time", "Contract", "Full-time"]),
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 7)),
            "posted_date": _rand_date(45), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 6 - European Cloud & Platform Engineering
# ---------------------------------------------------------------------------
def gen_dataset_6_europe_cloud(n: int = 25) -> List[Dict]:
    titles = [
        "Senior Platform Engineer", "Site Reliability Engineer", "Cloud Infrastructure Architect",
        "DevSecOps Engineer", "Kubernetes Reliability Engineer", "FinOps Cloud Analyst",
        "MLOps Platform Developer", "Service Mesh Engineer", "Observability Lead",
        "Data Platform Reliability Engineer", "Cloud Cost Optimiser", "GitOps Engineer",
        "Internal Developer Platform Lead", "Chaos Engineering Specialist", "Edge Computing Architect"
    ]
    companies = ["Spotify", "Zalando", "Booking.com", "ASML", "SAP", "Siemens", "N26", "Klarna",
                 "Delivery Hero", "Auto1 Group", "Personio", "Celonis", "TeamViewer", "Adidas", "BASF Digital"]
    locations = ["Berlin, DE", "Amsterdam, NL", "Stockholm, SE", "Munich, DE", "Barcelona, ES",
                 "London, UK", "Dublin, IE", "Warsaw, PL", "Lisbon, PT", "Copenhagen, DK"]
    levels = ["Mid", "Senior", "Lead", "Staff"]
    skills_pool = ["Kubernetes", "Terraform", "Helm", "ArgoCD", "Prometheus", "Grafana", "Go", "Python",
                   "Rust", "AWS", "GCP", "Azure", "Istio", "Linkerd", "Cilium", "Vault", "OPA", "Pulumi",
                   "OpenTelemetry", "Crossplane", "Dagger", "GitHub Actions", "Datadog", "eBPF"]
    sources = ["LinkedIn", "Welcome to the Jungle", "Stack Overflow Jobs", "Glassdoor", "Indeed"]
    descs = [
        "Own platform reliability for 50M+ active users across 40 European markets",
        "Drive FinOps maturity and reduce cloud spend by 30% while improving SLOs",
        "Implement internal developer platform using Backstage and GitOps workflows",
        "Build production-grade eBPF-based networking and security observability",
        "Lead chaos engineering practice and game-day exercises for critical services",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Mid": 75000, "Senior": 100000, "Lead": 130000, "Staff": 160000}[lvl]
        base_hi = base_lo + 40000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi, 10000), "job_type": "Full-time",
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 7)),
            "posted_date": _rand_date(50), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 7 - India GCC Deep Tech
# ---------------------------------------------------------------------------
def gen_dataset_7_india_gcc_deeptech(n: int = 25) -> List[Dict]:
    titles = [
        "Principal Data Engineer", "AI/ML Platform Architect", "VLSI Design Engineer",
        "Embedded Systems Engineer", "Autonomous Vehicle Data Engineer", "Semiconductor Process Engineer",
        "Robotics Firmware Engineer", "ASIC Verification Engineer", "Computer Vision Researcher",
        "Edge AI Deployment Engineer", "Digital Twin Data Architect", "5G Network Data Engineer",
        "Quantum Computing Research Engineer", "Photonics Research Scientist", "HPC Systems Engineer"
    ]
    companies = ["Qualcomm India", "Intel India", "Texas Instruments", "Bosch India", "Continental India",
                 "Honeywell Connected Enterprise", "ABB India", "Siemens DI", "GE Vernova", "Caterpillar GITC",
                 "Mercedes-Benz R&D India", "Toyota Connected India", "Micron Technology India", "NVIDIA India", "AMD India"]
    locations = ["Bangalore, KA", "Hyderabad, TS", "Pune, MH", "Chennai, TN", "Noida, UP",
                 "Gurugram, HR", "Coimbatore, TN", "Mysore, KA", "Trivandrum, KL", "Chandigarh, PB"]
    levels = ["Mid", "Senior", "Lead", "Principal", "Staff"]
    skills_pool = ["C++", "Python", "SystemVerilog", "VHDL", "ROS2", "CUDA", "TensorRT", "OpenCL",
                   "MATLAB", "LabVIEW", "Verilog", "Linux Kernel", "RTOS", "Qt", "CMake", "Docker",
                   "PyTorch", "ONNX", "OpenCV", "PCIe", "DDR5", "ARM Cortex", "RISC-V", "Ansys"]
    sources = ["Naukri.com", "LinkedIn India", "Instahyre", "Cutshort", "Hirist.com"]
    descs = [
        "Architect scalable data pipelines for billion-parameter ML model training on HPC clusters",
        "Design ADAS perception model optimization for automotive edge inference at 30fps",
        "Build VLSI design verification frameworks reducing tape-out defect rates by 40%",
        "Develop 5G network slice telemetry data lake for QoS and SLA analytics",
        "Lead robotics simulation digital twin platform using NVIDIA Isaac Sim and ROS2",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base = {"Mid": 2000000, "Senior": 3000000, "Lead": 4200000, "Principal": 5500000, "Staff": 7000000}[lvl]
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _inr_salary(base, base + 1500000), "job_type": _pick(["Full-time", "Full-time", "Contract"]),
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 7)),
            "posted_date": _rand_date(40), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 8 - US Healthcare & Life Sciences Data
# ---------------------------------------------------------------------------
def gen_dataset_8_us_healthcare(n: int = 25) -> List[Dict]:
    titles = [
        "Clinical Data Engineer", "Bioinformatics Data Scientist", "Healthcare Interoperability Engineer",
        "EHR Platform Engineer", "Genomics Pipeline Developer", "Real-World Evidence Analyst",
        "Health AI Product Engineer", "Medical Imaging ML Engineer", "FHIR API Developer",
        "Drug Safety Data Scientist", "Clinical Trials Data Architect", "Payer Analytics Engineer",
        "Population Health Data Engineer", "Digital Biomarker Researcher", "Precision Medicine Engineer"
    ]
    companies = ["Epic Systems", "Veeva Systems", "Flatiron Health", "Tempus AI", "Komodo Health",
                 "Medidata Solutions", "IQVIA", "Optum", "CVS Health", "Abbott",
                 "Illumina", "Pacific Biosciences", "Recursion Pharmaceuticals", "Color Genomics", "Clover Health"]
    locations = ["Boston, MA", "San Francisco, CA", "New York, NY", "San Diego, CA", "Raleigh, NC",
                 "Chicago, IL", "Philadelphia, PA", "Minneapolis, MN", "Houston, TX", "Baltimore, MD"]
    levels = ["Entry", "Mid", "Senior", "Lead", "Principal"]
    skills_pool = ["Python", "R", "SQL", "FHIR", "HL7", "Spark", "Nextflow", "Snakemake", "GATK",
                   "Bioconductor", "Seurat", "PyTorch", "MONAI", "DICOM", "AWS HealthLake",
                   "Databricks", "Snowflake", "dbt", "Airflow", "Docker", "Terra", "SAS", "Stata"]
    sources = ["LinkedIn", "Indeed", "Glassdoor", "Health IT Jobs", "MedReps"]
    descs = [
        "Design scalable NGS variant calling pipeline processing 1000 genomes weekly",
        "Build FHIR-compliant interoperability hub connecting 200+ hospital EHR systems",
        "Develop real-world evidence platform for oncology drug comparative effectiveness studies",
        "Create population health analytics for value-based care 50M member contracts",
        "Implement federated learning for multi-site clinical trial data privacy preservation",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Entry": 80000, "Mid": 110000, "Senior": 145000, "Lead": 175000, "Principal": 210000}[lvl]
        base_hi = base_lo + 50000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi), "job_type": "Full-time",
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 7)),
            "posted_date": _rand_date(55), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 9 - APAC E-Commerce & Logistics Tech
# ---------------------------------------------------------------------------
def gen_dataset_9_apac_ecommerce(n: int = 25) -> List[Dict]:
    titles = [
        "Supply Chain Data Engineer", "Last-Mile Logistics Optimiser", "Search & Recommendations Engineer",
        "A/B Testing Platform Engineer", "Customer Data Platform Engineer", "Marketplace Fraud Engineer",
        "Inventory Forecasting Scientist", "Pricing Intelligence Engineer", "Seller Analytics Developer",
        "Cross-Border Payments Engineer", "Warehouse Robotics Data Engineer", "Real-Time Delivery Tracker",
        "Demand Planning Data Scientist", "Catalogue ML Engineer", "Growth Analytics Lead"
    ]
    companies = ["Sea Limited (Shopee)", "Lazada", "Grab", "GoTo (Tokopedia)", "Coupang",
                 "JD.com APAC", "Ninja Van", "J&T Express", "Naver Commerce", "Kakao Commerce",
                 "Tiki", "Bukalapak", "Paidy", "Afterpay APAC", "99 Group"]
    locations = ["Singapore, SG", "Jakarta, ID", "Bangkok, TH", "Ho Chi Minh City, VN",
                 "Manila, PH", "Kuala Lumpur, MY", "Seoul, KR", "Tokyo, JP", "Sydney, AU", "Taipei, TW"]
    levels = ["Mid", "Senior", "Lead", "Staff"]
    skills_pool = ["Python", "Spark", "Kafka", "Flink", "SQL", "Airflow", "dbt", "ClickHouse",
                   "Redis", "Go", "Java", "Kubernetes", "GCP", "AWS", "BigQuery", "Pinot",
                   "Druid", "Hudi", "Iceberg", "Trino", "Superset", "MLflow", "LightGBM"]
    sources = ["LinkedIn", "Glints", "Tech In Asia Jobs", "Jobstreet", "Indeed"]
    descs = [
        "Build real-time inventory availability and demand forecasting for 50M SKUs",
        "Design sub-second fraud detection for 10M daily checkout transactions",
        "Architect event-driven logistics ETAs using GPS telemetry and ML routing models",
        "Develop personalised search ranking pipeline for 100M+ monthly active shoppers",
        "Create cross-border payment settlement reconciliation data infrastructure",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Mid": 50000, "Senior": 80000, "Lead": 110000, "Staff": 140000}[lvl]
        base_hi = base_lo + 35000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi, 8000), "job_type": "Full-time",
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 7)),
            "posted_date": _rand_date(45), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 10 - US EdTech & Learning Platforms
# ---------------------------------------------------------------------------
def gen_dataset_10_us_edtech(n: int = 20) -> List[Dict]:
    titles = [
        "Learning Data Engineer", "EdTech ML Scientist", "Adaptive Learning Platform Developer",
        "Assessment Analytics Engineer", "Student Engagement Data Analyst", "Curriculum Recommendation Engineer",
        "LMS Data Architect", "Gamification Data Scientist", "Tutoring AI Engineer",
        "Academic Integrity ML Engineer", "Parent & Student Dashboard Developer", "Teacher Tools Data Engineer"
    ]
    companies = ["Coursera", "2U / edX", "Chegg", "Khan Academy", "Duolingo", "Renaissance Learning",
                 "Instructure (Canvas)", "Pearson Digital", "McGraw Hill Digital", "Newsela",
                 "Amplify Education", "Noodle Partners", "Parchment", "PowerSchool", "ClassDojo"]
    locations = ["San Francisco, CA", "New York, NY", "Boston, MA", "Pittsburgh, PA", "Austin, TX",
                 "Remote, US", "Chicago, IL", "Denver, CO", "Los Angeles, CA", "Seattle, WA"]
    levels = ["Entry", "Mid", "Senior", "Lead"]
    skills_pool = ["Python", "SQL", "dbt", "Snowflake", "Airbyte", "Airflow", "Spark",
                   "Looker", "Tableau", "BigQuery", "AWS", "Segment", "Amplitude", "Mixpanel",
                   "PostgreSQL", "MongoDB", "Redis", "FastAPI", "React", "GraphQL", "Kafka"]
    sources = ["LinkedIn", "Indeed", "EdSurge Jobs", "Glassdoor", "Built In"]
    descs = [
        "Build learner engagement scoring pipeline processing 20M daily activity events",
        "Design adaptive content recommendation engine using IRT and collaborative filtering",
        "Create student outcome prediction models for early intervention programs",
        "Implement privacy-safe (FERPA/COPPA) data infrastructure for K-12 analytics",
        "Develop real-time dashboards for teachers to track class mastery in live sessions",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Entry": 75000, "Mid": 100000, "Senior": 130000, "Lead": 160000}[lvl]
        base_hi = base_lo + 45000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi), "job_type": _pick(["Full-time", "Full-time", "Remote-First"]),
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 6)),
            "posted_date": _rand_date(50), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 11 - India Startup Product & Growth Engineering
# ---------------------------------------------------------------------------
def gen_dataset_11_india_startup_product(n: int = 22) -> List[Dict]:
    titles = [
        "Product Analytics Engineer", "Growth Data Scientist", "Full-Stack Data Engineer",
        "Backend Engineer (Data-heavy)", "Revenue Analytics Lead", "Conversion Rate Optimiser",
        "Funnel Analytics Developer", "User Lifecycle Data Scientist", "Attribution Data Engineer",
        "Experimentation Platform Engineer", "Monetisation Analytics Engineer", "Mobile Analytics Developer"
    ]
    companies = ["CRED", "BharatPe", "Slice", "Jupiter", "Fi Money", "Juspay", "Setu",
                 "Recko", "GoKwik", "Shiprocket", "Delhivery Tech", "Yellow.ai",
                 "Darwinbox", "LeadSquared", "Freshworks", "Chargebee", "Sarvam AI", "Krutrim"]
    locations = ["Bangalore, KA", "Mumbai, MH", "Delhi NCR", "Hyderabad, TS", "Pune, MH", "Chennai, TN"]
    levels = ["Entry", "Mid", "Senior"]
    skills_pool = ["Python", "SQL", "dbt", "Metabase", "Redash", "Mixpanel", "Amplitude", "Segment",
                   "Kafka", "Airflow", "FastAPI", "PostgreSQL", "ClickHouse", "Redis",
                   "GCP", "BigQuery", "Looker", "Superset", "Spark", "Docker"]
    sources = ["Instahyre", "AngelList India", "LinkedIn India", "Cutshort", "Naukri.com"]
    descs = [
        "Build product funnel analytics tracking 5M+ daily user events with sub-1s latency",
        "Design cohort-based LTV prediction models to optimise subscription monetisation",
        "Create experimentation platform with multi-variate testing for 100+ concurrent experiments",
        "Develop attribution data pipeline integrating Google Ads, Meta, and AppsFlyer",
        "Architect event schema governance and data contract system for product analytics",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base = {"Entry": 1000000, "Mid": 1800000, "Senior": 3000000}[lvl]
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _inr_salary(base, base + 1200000), "job_type": _pick(["Full-time", "Full-time", "Hybrid"]),
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 6)),
            "posted_date": _rand_date(35), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 12 - US Defense, Aerospace & Government Data
# ---------------------------------------------------------------------------
def gen_dataset_12_us_defense_govt(n: int = 20) -> List[Dict]:
    titles = [
        "Geospatial Data Engineer", "Satellite Imagery ML Engineer", "Intelligence Analyst (Data)",
        "SIGINT Data Scientist", "Cyber Threat Intelligence Engineer", "Mission Data Architect",
        "Autonomous Systems Data Engineer", "Radar Signal Processing Engineer",
        "Digital Engineering Systems Analyst", "Classified Cloud Engineer (TS/SCI)",
        "Electronic Warfare Data Scientist", "Space Domain Analytics Lead"
    ]
    companies = ["Palantir", "Booz Allen Hamilton", "Leidos", "SAIC", "L3Harris", "Northrop Grumman",
                 "Raytheon Technologies", "General Dynamics IT", "MITRE Corporation", "Accenture Federal",
                 "Peraton", "Maxar Technologies", "Planet Labs", "SpaceX", "SES Government Solutions"]
    locations = ["Washington DC, DC", "Arlington, VA", "Colorado Springs, CO", "San Diego, CA",
                 "Huntsville, AL", "Dayton, OH", "Orlando, FL", "Chantilly, VA", "Annapolis, MD", "El Segundo, CA"]
    levels = ["Mid", "Senior", "Lead", "Principal"]
    skills_pool = ["Python", "C++", "MATLAB", "AWS GovCloud", "Azure Government", "Spark",
                   "TensorFlow", "PyTorch", "GDAL", "OpenCV", "PostGIS", "QGIS", "Kafka",
                   "ElasticSearch", "Kubernetes", "Docker", "Ansible", "Terraform", "OSINT Tools"]
    sources = ["ClearanceJobs", "LinkedIn", "USAJobs", "Indeed", "Glassdoor"]
    descs = [
        "Build multi-source geospatial fusion pipeline for national security satellite imagery",
        "Develop RF signal classification ML models for spectrum awareness systems",
        "Architect zero-trust data mesh for cross-agency intelligence sharing",
        "Create synthetic aperture radar change detection algorithms for persistent surveillance",
        "Implement cyber threat detection SIEM pipelines processing 5TB/day of network telemetry",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Mid": 110000, "Senior": 145000, "Lead": 180000, "Principal": 210000}[lvl]
        base_hi = base_lo + 60000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi), "job_type": "Full-time",
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 6)),
            "posted_date": _rand_date(60), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 13 - Global Climate Tech & Sustainability Data
# ---------------------------------------------------------------------------
def gen_dataset_13_climate_tech(n: int = 22) -> List[Dict]:
    titles = [
        "Carbon Data Engineer", "ESG Analytics Platform Developer", "Climate Risk Data Scientist",
        "Renewable Energy Data Engineer", "Smart Grid Data Architect", "EV Battery Analytics Engineer",
        "Scope 3 Emissions Analyst", "Satellite ESG Data Engineer", "Green Finance Data Scientist",
        "Nature-Based Solutions Data Analyst", "Water Resource Data Engineer", "Air Quality ML Engineer",
        "Climate Model Data Engineer", "Circular Economy Data Architect", "Impact Measurement Scientist"
    ]
    companies = ["Watershed", "Persefoni", "Pachama", "Rubicon Carbon", "Xpansiv", "South Pole",
                 "EcoVadis", "Climeworks", "Tomorrow.io", "Bright Power", "Arcadia Power",
                 "Orsted Digital", "Enel X", "Sunrun Data", "Rivian Data Science"]
    locations = ["San Francisco, CA", "New York, NY", "London, UK", "Copenhagen, DK", "Berlin, DE",
                 "Amsterdam, NL", "Oslo, NO", "Zurich, CH", "Toronto, CA", "Stockholm, SE"]
    levels = ["Entry", "Mid", "Senior", "Lead"]
    skills_pool = ["Python", "R", "SQL", "Spark", "dbt", "Airflow", "Snowflake", "BigQuery",
                   "GDAL", "NetCDF4", "Xarray", "PVLIB", "OpenMeteo API", "Tableau", "Looker",
                   "Terraform", "AWS", "GCP", "Docker", "PostgreSQL", "TimescaleDB", "InfluxDB"]
    sources = ["LinkedIn", "ClimateBase", "GreenBiz Jobs", "Indeed", "Wellfound"]
    descs = [
        "Build GHG emission factor lookup and Scope 1/2/3 calculation pipeline",
        "Design satellite imagery processing pipeline for deforestation carbon credit verification",
        "Create smart meter telemetry data pipeline for 500K residential energy customers",
        "Develop climate scenario risk modelling for real estate portfolio stress testing",
        "Architect carbon credit registry data integration and reconciliation platform",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Entry": 75000, "Mid": 105000, "Senior": 135000, "Lead": 165000}[lvl]
        base_hi = base_lo + 45000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi), "job_type": _pick(["Full-time", "Full-time", "Remote-First"]),
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 6)),
            "posted_date": _rand_date(40), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 14 - India Remote & Freelance Global Tech
# ---------------------------------------------------------------------------
def gen_dataset_14_india_remote_global(n: int = 22) -> List[Dict]:
    titles = [
        "Remote Data Engineer (US Shift)", "Freelance ML Engineer", "Contract Spark Developer",
        "Remote dbt Analytics Engineer", "Freelance Cloud Architect (AWS)", "Contract Kafka Engineer",
        "Remote Platform SRE", "Freelance LLM Engineer", "Contract RAG Systems Developer",
        "Remote AI Safety Engineer", "Freelance Data Quality Engineer", "Contract Streaming Architect"
    ]
    companies = ["Toptal", "Arc.dev", "Gun.io", "Contra", "Deel Remote Teams", "Remote.com",
                 "CrossBorderWorks", "Turing.com", "Andela India", "Lemon.io", "Gigster", "Braintrust"]
    locations = ["Remote, India", "Remote (US Client)", "Remote (EU Client)", "Bangalore, KA (Remote)",
                 "Hyderabad, TS (Remote)", "Delhi NCR (Remote)", "Mumbai, MH (Remote)"]
    levels = ["Mid", "Senior", "Lead", "Principal"]
    skills_pool = ["Python", "Spark", "Kafka", "dbt", "Airflow", "Snowflake", "AWS", "GCP",
                   "Docker", "Kubernetes", "Terraform", "SQL", "FastAPI", "LangChain", "LlamaIndex",
                   "OpenAI API", "PostgreSQL", "Redis", "ClickHouse", "Iceberg", "Hudi", "Trino"]
    sources = ["Toptal", "LinkedIn", "Upwork", "Arc.dev", "Turing.com"]
    descs = [
        "Build production RAG knowledge base pipeline for US SaaS company",
        "Design multi-cloud data mesh architecture for European fintech client",
        "Develop real-time Kafka streaming ETL for US healthcare platform",
        "Architect LLM-powered data quality monitoring system for global retail client",
        "Create end-to-end dbt transformation layer for data warehouse modernisation",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base = {"Mid": 3500000, "Senior": 5000000, "Lead": 7000000, "Principal": 9000000}[lvl]
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _inr_salary(base, base + 2000000), "job_type": _pick(["Contract", "Freelance", "Full-time Remote"]),
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(4, 7)),
            "posted_date": _rand_date(30), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# DATASET 15 - Global AI/LLM Research & Engineering
# ---------------------------------------------------------------------------
def gen_dataset_15_global_ai_llm(n: int = 25) -> List[Dict]:
    titles = [
        "LLM Pre-Training Engineer", "AI Alignment Researcher", "RLHF/RLAIF Data Engineer",
        "Foundation Model Infrastructure Lead", "Multimodal AI Researcher", "AI Evaluation Engineer",
        "Red-Teaming & Safety Data Scientist", "Synthetic Data Pipeline Engineer",
        "AI Inference Optimization Engineer", "Prompt Engineering Lead", "Neural Architecture Researcher",
        "Distributed Training Systems Engineer", "Vector Database Infrastructure Engineer",
        "Embedding & Retrieval ML Engineer", "AI Product Safety Analyst"
    ]
    companies = ["Anthropic", "OpenAI", "Google DeepMind", "Meta AI", "Mistral AI", "Cohere",
                 "AI21 Labs", "Stability AI", "Runway ML", "Ideogram", "Perplexity AI",
                 "Together AI", "Hugging Face", "Inflection AI", "xAI"]
    locations = ["San Francisco, CA", "New York, NY", "London, UK", "Paris, FR", "Tel Aviv, IL",
                 "Cambridge, MA", "Seattle, WA", "Montreal, CA", "Berlin, DE", "Remote, Global"]
    levels = ["Mid", "Senior", "Lead", "Staff", "Principal"]
    skills_pool = ["Python", "PyTorch", "JAX", "CUDA", "Triton", "DeepSpeed", "FSDP", "Megatron-LM",
                   "Transformers", "vLLM", "TensorRT-LLM", "ONNX", "Ray", "Kubernetes",
                   "AWS", "GCP", "Azure", "Wandb", "MLflow", "FAISS", "Weaviate", "Qdrant",
                   "LangChain", "LlamaIndex", "Hugging Face", "C++", "Go", "Rust"]
    sources = ["LinkedIn", "Levels.fyi Jobs", "Wellfound", "Greenhouse", "Indeed"]
    descs = [
        "Pre-train and fine-tune 70B+ parameter LLMs using distributed FSDP training clusters",
        "Design synthetic data generation pipelines for instruction tuning and preference optimization",
        "Build automated red-teaming evaluation harness for AI safety benchmarking",
        "Architect high-throughput inference serving infrastructure achieving <100ms P99 latency",
        "Develop multimodal embedding pipeline for image-text retrieval at billion-item scale",
    ]
    rows = []
    for _ in range(n):
        lvl = _pick(levels)
        base_lo = {"Mid": 160000, "Senior": 220000, "Lead": 280000, "Staff": 340000, "Principal": 400000}[lvl]
        base_hi = base_lo + 80000
        rows.append({
            "job_title": _pick(titles), "company": _pick(companies), "location": _pick(locations),
            "salary": _salary(base_lo, base_hi, 20000), "job_type": "Full-time",
            "experience_level": lvl, "skills": _skills(skills_pool, random.randint(5, 8)),
            "posted_date": _rand_date(25), "source": _pick(sources), "description": _pick(descs),
        })
    return rows


# ---------------------------------------------------------------------------
# Writer & Registry
# ---------------------------------------------------------------------------

DATASET_REGISTRY = [
    ("dataset_5_us_fintech_banking.csv",          gen_dataset_5_us_fintech),
    ("dataset_6_europe_cloud_platform.csv",        gen_dataset_6_europe_cloud),
    ("dataset_7_india_gcc_deeptech.csv",           gen_dataset_7_india_gcc_deeptech),
    ("dataset_8_us_healthcare_lifesciences.csv",   gen_dataset_8_us_healthcare),
    ("dataset_9_apac_ecommerce_logistics.csv",     gen_dataset_9_apac_ecommerce),
    ("dataset_10_us_edtech_learning.csv",          gen_dataset_10_us_edtech),
    ("dataset_11_india_startup_product.csv",       gen_dataset_11_india_startup_product),
    ("dataset_12_us_defense_govt.csv",             gen_dataset_12_us_defense_govt),
    ("dataset_13_climate_tech_esg.csv",            gen_dataset_13_climate_tech),
    ("dataset_14_india_remote_global.csv",         gen_dataset_14_india_remote_global),
    ("dataset_15_global_ai_llm.csv",               gen_dataset_15_global_ai_llm),
]


def write_dataset(filename: str, rows: List[Dict]) -> str:
    path = os.path.join(STREAMING_DIR, filename)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDNAMES)
        writer.writeheader()
        writer.writerows(rows)
    logger.info(f"  Written {len(rows):>3} rows -> {filename}")
    return path


def generate_all_datasets(regenerate: bool = False) -> List[str]:
    """
    Generate all datasets. If regenerate=True, always recreates files.
    Returns list of file paths that were written/verified.
    """
    written = []
    logger.info(f"Generating {len(DATASET_REGISTRY)} diverse streaming datasets -> {STREAMING_DIR}")
    for filename, gen_fn in DATASET_REGISTRY:
        path = os.path.join(STREAMING_DIR, filename)
        if os.path.exists(path) and not regenerate:
            logger.info(f"  Skipping {filename} (already exists; pass regenerate=True to overwrite)")
            written.append(path)
            continue
        rows = gen_fn()
        write_dataset(filename, rows)
        written.append(path)

    logger.info(f"Dataset generation complete. {len(written)} datasets ready for streaming.")
    return written


def list_all_streaming_datasets() -> List[str]:
    """Returns sorted list of all CSV files in the streaming directory."""
    return sorted(f for f in os.listdir(STREAMING_DIR) if f.endswith(".csv"))


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Generate JobPulse streaming datasets")
    parser.add_argument("--regenerate", action="store_true", help="Regenerate even if files exist")
    parser.add_argument("--list", action="store_true", help="List existing datasets")
    args = parser.parse_args()

    if args.list:
        files = list_all_streaming_datasets()
        print(f"\nStreaming Datasets ({len(files)}):")
        for f in files:
            p = os.path.join(STREAMING_DIR, f)
            size = os.path.getsize(p)
            print(f"   {f}  ({size:,} bytes)")
    else:
        paths = generate_all_datasets(regenerate=args.regenerate)
        print(f"\nReady! {len(paths)} datasets in: {STREAMING_DIR}")
