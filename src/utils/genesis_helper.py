import pandas as pd


def pre_process_hospitals_annual(df: pd.DataFrame):
    tmp = df.copy()
    cols = [
        'iso_year',
        'amount_hospitals',
        'amount_beds',
        'amount_beds_per_100000_population',
        'amount_patients',
        'amount_patients_per_100000_population',
        'occupancy_days',
        'avg_days_of_hospitalization',
        'avg_bed_occupancy_percent'
    ]
    tmp.columns = cols
    return tmp


def pre_process_hospital_staff_annual(df: pd.DataFrame):
    tmp = df.copy()
    cols = [
        'iso_year',
        'total_staff',
        'full_time_doctors',
        'non_medical_staff',
        'non_medical_staff_in_nursing_service'
    ]
    tmp.columns = cols
    return tmp
