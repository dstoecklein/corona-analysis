from src.read_config import read_yaml
from src.get_data import rki

config = read_yaml()
config_cols = read_yaml('config_cols.yaml')



df = rki(
    url=config['urls']['rki_tests'],
    purpose='TESTS',
    save_file=False,
    path='',
    is_excel=True,
    sheet_name='1_Testzahlerfassung'
)
df.columns = config_cols['rki']['tests']['translation']

print(df.head())
