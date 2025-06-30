# IASO Organizational Units Extraction Pipeline

## 📌 Description

This pipeline extracts organizational units data from the IASO platform based processes and cleans the data, and then exports it to the specified destination (file, database, or OpenHEXA dataset).
Key features:
- Authenticates with IASO using provided credentials
- Fetches organizational units data (optionally filtered by type)
- Handles geometry conversion for spatial data
- Exports to various file formats (CSV, GeoPackage, GeoJSON, Parquet, Shapefile, TopoJSON, Excel)
- Supports database export to spatial databases (PostGIS)
- Integrates with OpenHexa Datasets for versioned data storage

## 💻 Usage Example
![run image](docs/images/example_run.png)

## ⚙️ Parameters

| Parameter | Type | Required | Default | Description |
|-----------|------|----------|---------|-------------|
| `iaso_connection` | IASOConnection | Yes | - | Authentication details for IASO (url, username, password) |
| `ou_type_id` | int | No | - | Specific organization unit type ID to filter results |
| `output_file_name` | str | No | - | Custom output path/filename (without extension) |
| `output_format` | str | No | .gpkg | Export file format (`.csv`, `.gpkg`, `.geojson`, `.parquet`, `.shp`, `.topojson`, `.xlsx`)|
| `db_table_name` | str | No | - | Target database table name for storage |
| `save_mode` | str | No | - | Database write mode ('append' or 'replace') |
| `dataset` | Dataset | No | - | Target OpenHexa Dataset for export |


## 🔄 Pipeline Flow

```mermaid
graph TD
    A([Start Pipeline]) --> B[Authenticate with IASO]
    B --> C{Fetch Org Units}
    C -->|With OU Type ID| D[Filter by specific type]
    C -->|Without OU Type ID| E[Fetch all units]
    D --> F[Process Geometry]
    E --> F
    F --> G{Output Selection}
    G -->|File Export| H[Generate output path]
    H --> I[Convert to selected format]
    I --> J[Save to workspace]
    G -->|Database Export| K[Prepare GeoDataFrame]
    K --> L[Write to PostGIS]
    G -->|Dataset Export| M[Add to Dataset Version]
    J --> N([End Pipeline])
    L --> N
    M --> N
```


