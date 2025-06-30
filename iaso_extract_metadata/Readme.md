# IASO Form Metadata Extraction Pipeline

## 📌 Description

This pipeline extracts detailed metadata about IASO forms, including questions and choices definitions. Key features:
- Authenticates with IASO using provided credentials
- Retrieves form structure and question definitions
- Fetches choice options with labels and values
- Exports to multiple formats (CSV, Parquet, Excel)
- Supports database export with join between questions and choices
- Integrates with OpenHexa Datasets for versioned metadata storage

## 💻 Usage Example
![run image](docs/images/example_run.png)

## ⚙️ Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `iaso_connection` | IASOConnection | Yes | - | Authentication details for IASO (url, username, password) |
| `form_id` | int | Yes | - | ID of the form to extract metadata from |
| `output_file_name` | str | No | - | Custom output path/filename (without extension) |
| `output_format` | str | No | `.parquet` | Export file format (`.csv`, `.parquet`, `.xlsx`) |
| `db_table_name` | str | No | - | Target database table name for storage |
| `save_mode` | str | No | `replace` | Database write mode (`append` or `replace`) |
| `dataset` | Dataset | No | - | Target OpenHexa Dataset for export |


## Execution Workflow
```mermaid
graph TD
    A([Start Pipeline]) --> B[Authenticate with IASO]
    B --> C[Get Form Name]
    C --> D[Fetch Form Metadata]
    D --> E[Extract Latest Version]
    E --> F[Process Questions]
    E --> G[Process Choices]
    F --> H{Output Selection}
    G --> H
    H -->|File Export| I[Generate output path]
    I --> J[Export to selected format]
    H -->|Database Export| K[Join Questions/Choices]
    K --> L[Write to Database]
    H -->|Dataset Export| M[Add to Dataset Version]
    J --> N([End Pipeline])
    L --> N
    M --> N
```