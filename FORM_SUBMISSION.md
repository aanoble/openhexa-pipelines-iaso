# Form Submission API Documentation

## Overview

The `/sync/form_upload/` endpoint is used to upload form submissions (instances) from mobile applications or other clients. This endpoint accepts XML-formatted form data and optional file attachments.

## Endpoint Details

**URL:** `/sync/form_upload/`

**Method:** `POST`

**Content-Type:** `multipart/form-data`

**Authentication:** Optional
- JWT token via `Authorization` header (format: `Bearer {token}`)
- Standard Django session authentication
- Anonymous uploads are allowed (user will be detected from JWT if provided)

**CSRF:** Exempt (no CSRF token required)

## Request Format

### Required Fields

| Field Name | Type | Description |
|------------|------|-------------|
| `xml_submission_file` | File | The main XML file containing the form submission data in OpenRosa format |

### Optional Fields

Any additional files can be included in the request with custom field names. These will be saved as `InstanceFile` objects associated with the submission.

Common examples:
- Image attachments (e.g., `imgUrl`, `photo1.jpg`)
- Audio files
- Video files
- Other supporting documents

## Request Headers

### Standard Headers (from mobile app)

```
Content-Type: multipart/form-data
Accept-Encoding: gzip,deflate
X-OpenRosa-Version: 1.0
```

### Authentication Header (optional)

```
Authorization: Bearer {jwt_token}
```

## XML Submission Format

The XML file should follow the OpenRosa XForms format. Key elements include:

```xml
<?xml version='1.0' ?>
<form_name id="form_id" version="form_version">
    <formhub>
        <uuid>form_uuid</uuid>
    </formhub>
    <start>2019-12-02T14:07:52.465+01:00</start>
    <end>2019-12-02T14:10:11.380+01:00</end>
    <deviceid>device_identifier</deviceid>

    <!-- Form-specific fields -->
    <field1>value1</field1>
    <field2>value2</field2>

    <!-- GPS coordinates (if applicable) -->
    <gps>latitude longitude altitude accuracy</gps>

    <!-- Metadata -->
    <meta>
        <instanceID>uuid:unique_instance_id</instanceID>
    </meta>
</form_name>
```

## Response

### Success Response

**Status Code:** `201 Created`

**Response Body:**
```json
{
  "result": "success"
}
```

## Processing Flow

When a form submission is uploaded, the following steps occur:

1. **Instance Lookup/Creation**
   - System checks if an instance with the same file name already exists
   - If exists: Updates the existing instance
   - If not: Creates a new instance

2. **File Processing**
   - XML file is saved and parsed
   - JSON representation is extracted from XML
   - User information is attached (from authentication or JWT)

3. **Data Extraction**
   - Location data is converted from GPS field
   - Device information is extracted
   - Correlation ID is generated/updated (if form is correlatable)

4. **Attachment Handling**
   - All additional files are saved as `InstanceFile` objects
   - Files are linked to the instance

5. **Post-Processing**
   - If project has `INSTANT_EXPORT` feature enabled, instance is exported immediately
   - Modification is logged in audit trail

## Usage Examples

### Example 1: Basic Form Submission (Python)

```python
import requests

url = "https://your-iaso-instance.com/sync/form_upload/"

# Prepare the XML file
with open('submission.xml', 'rb') as xml_file:
    files = {
        'xml_submission_file': xml_file
    }

    response = requests.post(url, files=files)
    print(response.status_code)  # 201
    print(response.json())  # {"result": "success"}
```

### Example 2: Form Submission with Authentication and Attachments

```python
import requests

url = "https://your-iaso-instance.com/sync/form_upload/"

# Get JWT token first
login_response = requests.post(
    "https://your-iaso-instance.com/api/token/",
    json={"username": "user", "password": "pass"}
)
token = login_response.json()["access"]

# Prepare files
with open('submission.xml', 'rb') as xml_file, \
     open('photo.jpg', 'rb') as photo_file:

    files = {
        'xml_submission_file': xml_file,
        'imgUrl': photo_file  # Attachment with custom name
    }

    headers = {
        'Authorization': f'Bearer {token}',
        'X-OpenRosa-Version': '1.0'
    }

    response = requests.post(url, files=files, headers=headers)
    print(response.status_code)  # 201
```

### Example 3: Form Submission from Test (Django Test Client)

```python
from django.test import TestCase

class FormSubmissionTest(TestCase):
    def test_upload_form(self):
        with open('iaso/tests/fixtures/example.xml') as fp:
            response = self.client.post(
                '/sync/form_upload/',
                {'xml_submission_file': fp},
                format='multipart'
            )

        self.assertEqual(response.status_code, 201)
        self.assertEqual(response.json(), {"result": "success"})
```

### Example 4: cURL Command

```bash
curl -X POST \
  https://your-iaso-instance.com/sync/form_upload/ \
  -H 'Authorization: Bearer YOUR_JWT_TOKEN' \
  -H 'X-OpenRosa-Version: 1.0' \
  -F 'xml_submission_file=@/path/to/submission.xml' \
  -F 'photo.jpg=@/path/to/photo.jpg'
```

## Workflow Integration

### Typical Mobile App Workflow

1. **User fills out form** on mobile device (ODK Collect/IASO Mobile)
2. **Form is saved locally** as XML with a unique instance ID
3. **Metadata is sent** to `/api/instances/?app_id={app_id}` endpoint first
4. **XML file is uploaded** to `/sync/form_upload/` endpoint
5. **Attachments are included** in the same multipart request
6. **Server processes** and confirms with 201 status

### Before Form Upload

The mobile app typically sends instance metadata first:

```python
# Step 1: Send instance metadata
instance_metadata = [{
    "id": "uuid:4b7c3954-f69a-4b99-83b1-db73957b32d2",
    "latitude": 4.4,
    "longitude": 4.4,
    "accuracy": 10,
    "altitude": 100,
    "created_at": 1565258153704,
    "updated_at": 1565258153704,
    "orgUnitId": 123,
    "formId": 456,
    "file": "example.xml",
    "name": "example.xml"
}]

requests.post(
    f"/api/instances/?app_id={app_id}",
    json=instance_metadata
)

# Step 2: Upload actual XML file
with open('example.xml', 'rb') as f:
    requests.post('/sync/form_upload/', files={'xml_submission_file': f})
```

## Error Handling

The endpoint has minimal error handling:
- Invalid XML files will be silently ignored (try/except block)
- Missing `xml_submission_file` will raise a KeyError
- Processing errors are logged but don't prevent successful response

## Implementation Reference

- **View Function:** `hat/sync/views.py:form_upload` (line 61)
- **URL Pattern:** `hat/sync/urls.py` (line 6)
- **Mobile App Code:**
  - [InstanceServerUploaderTask.java](https://github.com/BLSQ/iaso-mobile-app/blob/0c6d821056e41d5beb6f745ea807451b07eb35f2/odk-collect/src/main/java/org/odk/collect/android/tasks/InstanceServerUploaderTask.java#L88)
  - [InstanceServerUploader.java](https://github.com/BLSQ/iaso-mobile-app/blob/0c6d821056e41d5beb6f745ea807451b07eb35f2/odk-collect/src/main/java/org/odk/collect/android/upload/InstanceServerUploader.java#L70)
  - [SyncInstances.kt](https://github.com/BLSQ/iaso-mobile-app/blob/0c6d821056e41d5beb6f745ea807451b07eb35f2/collect_app/src/main/java/com/bluesquare/iaso/usecase/SyncInstances.kt)

## Related Models

- **Instance** (`iaso.models.Instance`): Main form submission model
- **InstanceFile** (`iaso.models.InstanceFile`): File attachments linked to instances
- **Form** (`iaso.models.Form`): Form definition with correlation settings
- **Project** (`iaso.models.Project`): Project with feature flags like `INSTANT_EXPORT`

## Notes

- This endpoint exists for **historical reasons and compatibility** with mobile applications
- The file name is used as a unique identifier for duplicate detection
- Correlation IDs are automatically generated for correlatable forms
- GPS coordinates are automatically extracted and stored if present in the XML
- The endpoint is designed to work with OpenRosa-compatible clients

