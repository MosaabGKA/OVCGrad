import json
import csv
from collections import defaultdict

# ------------------------------------------------------------
# Helper to safely get a field, returning empty string if missing
# ------------------------------------------------------------
def get_field(obj, field, default=""):
    return obj.get(field, default)

# ------------------------------------------------------------
# Flatten a list into a semicolon-separated string for CSV storage
# ------------------------------------------------------------
def flatten_list(lst):
    if not lst:
        return ""
    return ";".join(str(item) for item in lst)

# ------------------------------------------------------------
# Load JSON data
# ------------------------------------------------------------
with open("clinical.project-tcga-ov.2026-03-15.json", "r", encoding="utf-8") as f:
    cases = json.load(f)

# ------------------------------------------------------------
# Prepare CSV writers and files
# ------------------------------------------------------------
patients_file = open("patients.csv", "w", newline="", encoding="utf-8")
diagnoses_file = open("diagnoses.csv", "w", newline="", encoding="utf-8")
treatments_file = open("treatments.csv", "w", newline="", encoding="utf-8")
pathology_file = open("pathology_details.csv", "w", newline="", encoding="utf-8")
followups_file = open("follow_ups.csv", "w", newline="", encoding="utf-8")

patients_writer = None
diagnoses_writer = None
treatments_writer = None
pathology_writer = None
followups_writer = None

# We'll define headers after we know all fields (or we can define them explicitly)
# For simplicity, we define headers explicitly.

# Patients headers
patients_headers = [
    "case_id", "submitter_id", "primary_site", "disease_type", "consent_type",
    "index_date", "state", "updated_datetime", "days_to_consent", "lost_to_followup",
    "ethnicity", "gender", "race", "vital_status", "age_at_index", "days_to_birth",
    "days_to_death", "population_group", "country_of_residence_at_enrollment"
]

# Diagnoses headers
diagnoses_headers = [
    "diagnosis_id", "case_id", "submitter_id", "days_to_diagnosis", "primary_diagnosis",
    "morphology", "icd_10_code", "classification_of_tumor", "diagnosis_is_primary_disease",
    "tissue_or_organ_of_origin", "site_of_resection_or_biopsy", "laterality", "figo_stage",
    "tumor_grade", "year_of_diagnosis", "age_at_diagnosis", "prior_malignancy",
    "synchronous_malignancy", "prior_treatment", "method_of_diagnosis", "residual_disease",
    "days_to_last_follow_up"
]

# Treatments headers
treatments_headers = [
    "treatment_id", "diagnosis_id", "case_id", "submitter_id", "treatment_type",
    "treatment_or_therapy", "treatment_intent_type", "initial_disease_status",
    "days_to_treatment_start", "days_to_treatment_end", "therapeutic_agents",
    "treatment_dose", "treatment_dose_units", "route_of_administration",
    "number_of_cycles", "number_of_fractions", "treatment_anatomic_sites",
    "treatment_outcome", "clinical_trial_indicator", "prescribed_dose",
    "prescribed_dose_units"
]

# Pathology details headers
pathology_headers = [
    "pathology_detail_id", "diagnosis_id", "case_id", "submitter_id",
    "consistent_pathology_review", "vascular_invasion_present",
    "lymphatic_invasion_present", "residual_tumor_measurement"
]

# Follow‑ups headers
followups_headers = [
    "follow_up_id", "case_id", "submitter_id", "timepoint_category",
    "days_to_follow_up", "disease_response", "progression_or_recurrence",
    "progression_or_recurrence_type", "evidence_of_recurrence_type",
    "days_to_recurrence", "ecog_performance_status", "karnofsky_performance_status"
]

# Create writers and write headers
patients_writer = csv.DictWriter(patients_file, fieldnames=patients_headers)
diagnoses_writer = csv.DictWriter(diagnoses_file, fieldnames=diagnoses_headers)
treatments_writer = csv.DictWriter(treatments_file, fieldnames=treatments_headers)
pathology_writer = csv.DictWriter(pathology_file, fieldnames=pathology_headers)
followups_writer = csv.DictWriter(followups_file, fieldnames=followups_headers)

patients_writer.writeheader()
diagnoses_writer.writeheader()
treatments_writer.writeheader()
pathology_writer.writeheader()
followups_writer.writeheader()

# ------------------------------------------------------------
# Iterate over cases and write rows
# ------------------------------------------------------------
for case in cases:
    case_id = get_field(case, "case_id")
    submitter_id = get_field(case, "submitter_id")
    primary_site = get_field(case, "primary_site")
    disease_type = get_field(case, "disease_type")
    consent_type = get_field(case, "consent_type")
    index_date = get_field(case, "index_date")
    state = get_field(case, "state")
    updated_datetime = get_field(case, "updated_datetime")
    days_to_consent = get_field(case, "days_to_consent")
    lost_to_followup = get_field(case, "lost_to_followup")

    # Demographic data
    demo = case.get("demographic", {})
    ethnicity = get_field(demo, "ethnicity")
    gender = get_field(demo, "gender")
    race = get_field(demo, "race")
    vital_status = get_field(demo, "vital_status")
    age_at_index = get_field(demo, "age_at_index")
    days_to_birth = get_field(demo, "days_to_birth")
    days_to_death = get_field(demo, "days_to_death")
    population_group = get_field(demo, "population_group")
    country = get_field(demo, "country_of_residence_at_enrollment")

    # Write patients row
    patients_writer.writerow({
        "case_id": case_id,
        "submitter_id": submitter_id,
        "primary_site": primary_site,
        "disease_type": disease_type,
        "consent_type": consent_type,
        "index_date": index_date,
        "state": state,
        "updated_datetime": updated_datetime,
        "days_to_consent": days_to_consent,
        "lost_to_followup": lost_to_followup,
        "ethnicity": ethnicity,
        "gender": gender,
        "race": race,
        "vital_status": vital_status,
        "age_at_index": age_at_index,
        "days_to_birth": days_to_birth,
        "days_to_death": days_to_death,
        "population_group": population_group,
        "country_of_residence_at_enrollment": country,
    })

    # Process diagnoses
    for diag in case.get("diagnoses", []):
        diagnosis_id = get_field(diag, "diagnosis_id")
        diag_submitter_id = get_field(diag, "submitter_id")
        days_to_diagnosis = get_field(diag, "days_to_diagnosis")
        primary_diagnosis = get_field(diag, "primary_diagnosis")
        morphology = get_field(diag, "morphology")
        icd_10_code = get_field(diag, "icd_10_code")
        classification_of_tumor = get_field(diag, "classification_of_tumor")
        diagnosis_is_primary_disease = get_field(diag, "diagnosis_is_primary_disease")
        tissue_or_organ_of_origin = get_field(diag, "tissue_or_organ_of_origin")
        site_of_resection_or_biopsy = get_field(diag, "site_of_resection_or_biopsy")
        laterality = get_field(diag, "laterality")
        figo_stage = get_field(diag, "figo_stage")
        tumor_grade = get_field(diag, "tumor_grade")
        year_of_diagnosis = get_field(diag, "year_of_diagnosis")
        age_at_diagnosis = get_field(diag, "age_at_diagnosis")
        prior_malignancy = get_field(diag, "prior_malignancy")
        synchronous_malignancy = get_field(diag, "synchronous_malignancy")
        prior_treatment = get_field(diag, "prior_treatment")
        method_of_diagnosis = get_field(diag, "method_of_diagnosis")
        residual_disease = get_field(diag, "residual_disease")
        days_to_last_follow_up = get_field(diag, "days_to_last_follow_up")

        diagnoses_writer.writerow({
            "diagnosis_id": diagnosis_id,
            "case_id": case_id,
            "submitter_id": diag_submitter_id,
            "days_to_diagnosis": days_to_diagnosis,
            "primary_diagnosis": primary_diagnosis,
            "morphology": morphology,
            "icd_10_code": icd_10_code,
            "classification_of_tumor": classification_of_tumor,
            "diagnosis_is_primary_disease": diagnosis_is_primary_disease,
            "tissue_or_organ_of_origin": tissue_or_organ_of_origin,
            "site_of_resection_or_biopsy": site_of_resection_or_biopsy,
            "laterality": laterality,
            "figo_stage": figo_stage,
            "tumor_grade": tumor_grade,
            "year_of_diagnosis": year_of_diagnosis,
            "age_at_diagnosis": age_at_diagnosis,
            "prior_malignancy": prior_malignancy,
            "synchronous_malignancy": synchronous_malignancy,
            "prior_treatment": prior_treatment,
            "method_of_diagnosis": method_of_diagnosis,
            "residual_disease": residual_disease,
            "days_to_last_follow_up": days_to_last_follow_up,
        })

        # Process treatments for this diagnosis
        for tr in diag.get("treatments", []):
            treatment_id = get_field(tr, "treatment_id")
            tr_submitter_id = get_field(tr, "submitter_id")
            treatment_type = get_field(tr, "treatment_type")
            treatment_or_therapy = get_field(tr, "treatment_or_therapy")
            treatment_intent_type = get_field(tr, "treatment_intent_type")
            initial_disease_status = get_field(tr, "initial_disease_status")
            days_to_treatment_start = get_field(tr, "days_to_treatment_start")
            days_to_treatment_end = get_field(tr, "days_to_treatment_end")
            therapeutic_agents = get_field(tr, "therapeutic_agents")
            treatment_dose = get_field(tr, "treatment_dose")
            treatment_dose_units = get_field(tr, "treatment_dose_units")
            route = flatten_list(tr.get("route_of_administration", []))
            number_of_cycles = get_field(tr, "number_of_cycles")
            number_of_fractions = get_field(tr, "number_of_fractions")
            anatomic_sites = flatten_list(tr.get("treatment_anatomic_sites", []))
            treatment_outcome = get_field(tr, "treatment_outcome")
            clinical_trial_indicator = get_field(tr, "clinical_trial_indicator")
            prescribed_dose = get_field(tr, "prescribed_dose")
            prescribed_dose_units = get_field(tr, "prescribed_dose_units")

            treatments_writer.writerow({
                "treatment_id": treatment_id,
                "diagnosis_id": diagnosis_id,
                "case_id": case_id,
                "submitter_id": tr_submitter_id,
                "treatment_type": treatment_type,
                "treatment_or_therapy": treatment_or_therapy,
                "treatment_intent_type": treatment_intent_type,
                "initial_disease_status": initial_disease_status,
                "days_to_treatment_start": days_to_treatment_start,
                "days_to_treatment_end": days_to_treatment_end,
                "therapeutic_agents": therapeutic_agents,
                "treatment_dose": treatment_dose,
                "treatment_dose_units": treatment_dose_units,
                "route_of_administration": route,
                "number_of_cycles": number_of_cycles,
                "number_of_fractions": number_of_fractions,
                "treatment_anatomic_sites": anatomic_sites,
                "treatment_outcome": treatment_outcome,
                "clinical_trial_indicator": clinical_trial_indicator,
                "prescribed_dose": prescribed_dose,
                "prescribed_dose_units": prescribed_dose_units,
            })

        # Process pathology details for this diagnosis
        for path in diag.get("pathology_details", []):
            path_id = get_field(path, "pathology_detail_id")
            path_submitter_id = get_field(path, "submitter_id")
            consistent_review = get_field(path, "consistent_pathology_review")
            vascular = get_field(path, "vascular_invasion_present")
            lymphatic = get_field(path, "lymphatic_invasion_present")
            residual_measure = get_field(path, "residual_tumor_measurement")

            pathology_writer.writerow({
                "pathology_detail_id": path_id,
                "diagnosis_id": diagnosis_id,
                "case_id": case_id,
                "submitter_id": path_submitter_id,
                "consistent_pathology_review": consistent_review,
                "vascular_invasion_present": vascular,
                "lymphatic_invasion_present": lymphatic,
                "residual_tumor_measurement": residual_measure,
            })

    # Process follow‑ups (directly under case, not under diagnoses)
    for fu in case.get("follow_ups", []):
        fu_id = get_field(fu, "follow_up_id")
        fu_submitter_id = get_field(fu, "submitter_id")
        timepoint = get_field(fu, "timepoint_category")
        days_to_fu = get_field(fu, "days_to_follow_up")
        disease_response = get_field(fu, "disease_response")
        prog_recur = get_field(fu, "progression_or_recurrence")
        prog_type = get_field(fu, "progression_or_recurrence_type")
        evidence = get_field(fu, "evidence_of_recurrence_type")
        days_to_recurrence = get_field(fu, "days_to_recurrence")
        ecog = get_field(fu, "ecog_performance_status")
        karnofsky = get_field(fu, "karnofsky_performance_status")

        followups_writer.writerow({
            "follow_up_id": fu_id,
            "case_id": case_id,
            "submitter_id": fu_submitter_id,
            "timepoint_category": timepoint,
            "days_to_follow_up": days_to_fu,
            "disease_response": disease_response,
            "progression_or_recurrence": prog_recur,
            "progression_or_recurrence_type": prog_type,
            "evidence_of_recurrence_type": evidence,
            "days_to_recurrence": days_to_recurrence,
            "ecog_performance_status": ecog,
            "karnofsky_performance_status": karnofsky,
        })

# ------------------------------------------------------------
# Close all files
# ------------------------------------------------------------
patients_file.close()
diagnoses_file.close()
treatments_file.close()
pathology_file.close()
followups_file.close()

print("Conversion complete. Generated files:")
print(" - patients.csv")
print(" - diagnoses.csv")
print(" - treatments.csv")
print(" - pathology_details.csv")
print(" - follow_ups.csv")
