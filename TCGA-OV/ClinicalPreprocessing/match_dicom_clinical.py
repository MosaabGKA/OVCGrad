import os
import json
import re
import csv
from collections import defaultdict
import pydicom
from pydicom.errors import InvalidDicomError

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
CLINICAL_JSON = "clinical.project-tcga-ov.2026-03-15.json"  # your clinical data
ANNOTATIONS_JSON = "annotations.2026-03-15.json"            # your annotations file
IMAGES_FOLDER = "../ImageCollection/dicom_images"           # folder with .dcm files
OUTPUT_CSV = "image_clinical_annotation_map.csv"

# ------------------------------------------------------------------
# Helper: extract patient ID from DICOM filename
# Expects pattern: patient_TCGA-XX-XXXX_...
# ------------------------------------------------------------------
def extract_patient_id_from_filename(filename):
    match = re.search(r'patient_(TCGA-\d{2}-\d{4})', filename)
    if match:
        return match.group(1)
    return None

# ------------------------------------------------------------------
# Load clinical data: dict keyed by submitter_id
# We'll also store the full case object for later use
# ------------------------------------------------------------------
def load_clinical_data(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        cases = json.load(f)
    clinical_lookup = {}
    for case in cases:
        submitter_id = case.get('submitter_id')
        if submitter_id:
            clinical_lookup[submitter_id] = case
    return clinical_lookup

# ------------------------------------------------------------------
# Load annotations: dict keyed by case_submitter_id -> list of annotations
# ------------------------------------------------------------------
def load_annotations(json_path):
    with open(json_path, 'r', encoding='utf-8') as f:
        annotations = json.load(f)
    ann_lookup = defaultdict(list)
    for ann in annotations:
        case_id = ann.get('case_submitter_id')
        if case_id:
            ann_lookup[case_id].append(ann)
    return ann_lookup

# ------------------------------------------------------------------
# Get primary diagnosis info from a case
# (the diagnosis where classification_of_tumor == 'primary')
# ------------------------------------------------------------------
def get_primary_diagnosis(case):
    for diag in case.get('diagnoses', []):
        if diag.get('classification_of_tumor') == 'primary':
            return {
                'diagnosis_id': diag.get('diagnosis_id', ''),
                'primary_diagnosis': diag.get('primary_diagnosis', ''),
                'figo_stage': diag.get('figo_stage', ''),
                'tumor_grade': diag.get('tumor_grade', ''),
                'icd_10_code': diag.get('icd_10_code', ''),
                'laterality': diag.get('laterality', ''),
                'days_to_diagnosis': diag.get('days_to_diagnosis', ''),
            }
    return {}

# ------------------------------------------------------------------
# Extract DICOM metadata (SeriesInstanceUID, StudyInstanceUID)
# ------------------------------------------------------------------
def get_dicom_identifiers(dicom_path):
    try:
        ds = pydicom.dcmread(dicom_path, stop_before_pixels=True)
        series_uid = getattr(ds, 'SeriesInstanceUID', '')
        study_uid = getattr(ds, 'StudyInstanceUID', '')
        return series_uid, study_uid
    except (InvalidDicomError, FileNotFoundError, AttributeError):
        return '', ''

# ------------------------------------------------------------------
# Main processing
# ------------------------------------------------------------------
def main():
    print("Loading clinical data...")
    clinical = load_clinical_data(CLINICAL_JSON)
    print(f"Loaded {len(clinical)} cases.")

    print("Loading annotations...")
    annotations = load_annotations(ANNOTATIONS_JSON)
    print(f"Loaded annotations for {len(annotations)} cases.")

    print(f"Scanning DICOM images in '{IMAGES_FOLDER}'...")
    dicom_files = [f for f in os.listdir(IMAGES_FOLDER) if f.lower().endswith('.dcm')]
    print(f"Found {len(dicom_files)} DICOM files.")

    # Prepare CSV output
    with open(OUTPUT_CSV, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = [
            'image_filename', 'full_path',
            'patient_id', 'case_id',
            'series_instance_uid', 'study_instance_uid',
            'primary_diagnosis', 'figo_stage', 'tumor_grade', 'icd_10_code', 'laterality',
            'diagnosis_id', 'days_to_diagnosis',
            'annotation_count', 'annotation_notes', 'annotation_categories'
        ]
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for filename in dicom_files:
            full_path = os.path.join(IMAGES_FOLDER, filename)
            patient_id = extract_patient_id_from_filename(filename)

            if not patient_id:
                print(f"Warning: Could not extract patient ID from {filename}")
                continue

            case = clinical.get(patient_id)
            if not case:
                print(f"Warning: No clinical data found for patient {patient_id} ({filename})")
                continue

            # Get primary diagnosis info
            diag_info = get_primary_diagnosis(case)

            # Get DICOM UIDs
            series_uid, study_uid = get_dicom_identifiers(full_path)

            # Get annotations for this patient
            ann_list = annotations.get(patient_id, [])
            ann_notes = []
            ann_categories = []
            for ann in ann_list:
                ann_notes.append(ann.get('notes', ''))
                ann_categories.append(ann.get('category', ''))
            annotation_count = len(ann_list)
            annotation_notes = '; '.join(ann_notes) if ann_notes else ''
            annotation_categories = '; '.join(set(ann_categories)) if ann_categories else ''

            row = {
                'image_filename': filename,
                'full_path': full_path,
                'patient_id': patient_id,
                'case_id': case.get('case_id', ''),
                'series_instance_uid': series_uid,
                'study_instance_uid': study_uid,
                'primary_diagnosis': diag_info.get('primary_diagnosis', ''),
                'figo_stage': diag_info.get('figo_stage', ''),
                'tumor_grade': diag_info.get('tumor_grade', ''),
                'icd_10_code': diag_info.get('icd_10_code', ''),
                'laterality': diag_info.get('laterality', ''),
                'diagnosis_id': diag_info.get('diagnosis_id', ''),
                'days_to_diagnosis': diag_info.get('days_to_diagnosis', ''),
                'annotation_count': annotation_count,
                'annotation_notes': annotation_notes,
                'annotation_categories': annotation_categories,
            }
            writer.writerow(row)

    print(f"Done. Mapping saved to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()
