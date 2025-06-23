# 🛠️ Medallion Data Pipeline - API Ingestion Case - Open Brewery DB

## 🎯 Objective  
This project aims to demonstrate the ingestion of data from an API ([Open Brewery DB](https://www.openbrewerydb.org/)), processing and storing the information in a Data Lake following the Medallion architecture (Bronze → Silver → Gold), ensuring data quality and scalability.

---

## 🧱 Layered Architecture  

- **Raw:** Raw data collected directly from the API, stored in JSON format.  
- **Curated:** Cleaned, validated, and partitioned data (e.g., by country and state), ready for intermediate analysis and consumption by other pipelines.  
- **Analytics:** Aggregated and optimized data for reporting and dashboards, focusing on performance for analytical queries. Data grouped by type, country, and state using the quantity of breweries.

---

## ⚙️ Technologies Used  

- **Orchestration:** Apache Airflow (manages pipeline execution, scheduling, and monitoring)  
- **Processing:** Python 3.10 + PySpark 3.5.1 (distributed processing for scalability; configured to run locally via Docker with Hadoop 3.3.1)  
- **Data Quality:** Deequ 1.5.0 (library for data quality and validation on Spark)  
- **Runtime Environment:** OpenJDK 11 (required by Hadoop and Deequ)  
- **Containerization:** Docker Compose (to facilitate service orchestration and provide reproducible environment)  
- **Dependency Management:** Poetry (precise control of Python libraries)  
- **Storage:** Local Data Lake
- **Schema Validation:** Pydantic (validation of extracted data)  
- **Testing:** Pytest (unit tests)  

---

## 💡 Design Choices & Trade-offs  

- **PySpark 3.5.1 + Hadoop 3.3.1:** Compatible with Deequ 1.5.0 and uses recent Spark features for local and cloud scalability.  
- **Docker Compose:** Simplifies replicating the development and testing environment but does not replace a real Spark cluster in production.
- **Deequ for validation:** Powerful for large-scale data quality checks but adds complexity and requires Java setup.  
- **Poetry:** Simplifies Python dependency and environment management but requires developer familiarity.  
- **Local Data Lake (Parquet):**  
  - Chosen to avoid creating cloud accounts and prevent potential cloud storage costs.  
  - **Trade-off:** Not scalable for production, lacks durability and accessibility compared to cloud-based solutions (e.g., S3, GCS).  
- **Airflow:** Provides robust scheduling and monitoring but requires infrastructure maintenance.

---

## 🗂️ Folder Structure  

```plaintext
├── airflow/                   # Airflow setup and logs
│   ├── dags/                  # Airflow DAGs
│   ├── logs/                  # Airflow logs
│   └── plugins/               # Custom Airflow plugins (if needed)
│
├── config/                    # Configuration files (YAMLs, settings, etc.)
│
├── datalake/                  # Local Data Lake (Bronze, Silver, Gold)
│   ├── bronze/
│   │   └── brewery/
│   │       └── dt_extraction=yyyy-MM-dd/
│   ├── silver/
│   │   └── brewery/
│   │       └── country=XX/
│   │           └── state=YY/
│   └── gold/
│       └── brewery/
│
├── pipelines/                 # ETL pipeline logic
│   ├── source-to-bronze/      # API ingestion and raw storage
│   ├── bronze-to-silver/      # Data cleansing and validation
│   └── silver-to-gold/        # Aggregation and analytics output
│
├── tests/                     # Unit and integration tests
│
├── .gitignore
├── .pre-commit-config.yaml    # Pre-commit hooks (e.g., Ruff, Pytest)
├── docker-compose.yaml        # Docker Compose setup
├── poetry.lock                # Poetry lock file
├── pyproject.toml             # Python project and dependencies
└── README.md                  # Project documentation
```

---

## 🚀 How to Run the Project  

### Prerequisites  

- Docker & Docker Compose  
- Poetry ([https://python-poetry.org/docs/](https://python-poetry.org/docs/))  
- Java JDK 11 available in your system path  
- Deequ ([https://mvnrepository.com/artifact/com.amazon.deequ/deequ/2.0.7-spark-3.5](https://mvnrepository.com/artifact/com.amazon.deequ/deequ/2.0.7-spark-3.5))

### 📦 Dependency Management

This project uses Poetry to manage Python dependencies via `pyproject.toml`.

Before building the Docker image or running the project, ensure that the `requirements.txt` is up to date.

To generate or update `requirements.txt` from your Poetry-managed dependencies, run:

```bash
poetry export -f requirements.txt --output requirements.txt --without-hashes
```

### Steps  

1. **Clone the repository:**  
   ```bash
   git clone <repo-url>
   cd <repo-folder>
   ```

2. **Install Python dependencies:**  
   ```bash
   poetry install
   ```

3. **Start the environment with Docker Compose:**  
   ```bash
   docker-compose up -d
   ```

4. **Access Airflow UI:**  
   Open your browser and go to [http://localhost:8080](http://localhost:8080)

5. **Trigger DAGs manually or wait for scheduled runs:**  
   - Source → Bronze: Fetches data from the API  
   - Bronze → Silver: Cleans, validates, and partitions the data  
   - Silver → Gold: Performs aggregations  

6. **Run tests:**  
   ```bash
   poetry run pytest
   ```

7. **(Optional) Run code quality checks with pre-commit:**  
   ```bash
   pre-commit run --all-files
   ```

---

## ⚠️ Notes  

- The local environment is intended for development and testing. For production, consider migrating to cloud-native services (e.g., EMR, Dataproc, cloud object storage).  
- Deequ metrics and reports (in JSON) can be exported from the validation step and reviewed for auditability.  
- All data is stored locally under the `datalake/` folder.
- **Validation and Testing Coverage**:
   Current data validations using PyDeequ and unit tests using Pytest cover only parts of the pipeline. Additional tests and validations should be added to increase coverage and ensure robustness across all pipeline stages.