import pandas as pd
import os
import glob

ENTITIES = ["FR", "PID", "CELSIUS", "VERTICAL"]

FEC_DIR = "data/fec"
MAPPING_FILE = "data/mappings/mapping_pcg.xlsx"
SPLIT_CA_COGS_FILE = "data/inputs/split_ca_cogs.xlsx"
SPLIT_RH_FILE = "data/inputs/split_rh.xlsx"


def load_fec(entity: str, period: str) -> pd.DataFrame:
    """
    Charge le FEC d'une entité pour une période donnée.
    period format : YYYYMM (ex: '202512')
    """
    pattern = os.path.join(FEC_DIR, f"FEC_{period}_{entity}.txt")
    files = glob.glob(pattern)
    if not files:
        raise FileNotFoundError(f"FEC introuvable : {pattern}")
    
    df = pd.read_csv(
        files[0],
        sep="\t",
        encoding="latin-1",
        dtype={"CompteNum": str},
        decimal=","
    )

    # Nettoyage colonnes
    df.columns = df.columns.str.strip()

    # Conversion des montants
    for col in ["Debit", "Credit"]:
        if df[col].dtype == object:
            df[col] = df[col].astype(str).str.replace(",", ".").str.replace(" ", "")
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    # Conversion date
    df["EcritureDate"] = pd.to_datetime(df["EcritureDate"], format="%Y%m%d", errors="coerce")

    # Montant net (Credit - Debit pour les comptes de produits, Debit - Credit pour charges)
    # On garde Debit et Credit séparés, le signe sera géré dans transformations.py
    df["CompteNum"] = df["CompteNum"].astype(str).str.strip()

    return df


def load_all_fec(period: str) -> dict[str, pd.DataFrame]:
    """Charge les FEC de toutes les entités pour une période."""
    fecs = {}
    for entity in ENTITIES:
        try:
            fecs[entity] = load_fec(entity, period)
            print(f"✅ FEC chargé : {entity} - {period}")
        except FileNotFoundError as e:
            print(f"⚠️  {e}")
    return fecs


def load_mapping(entity: str) -> pd.DataFrame:
    """Charge le mapping PCG d'une entité depuis le fichier Excel."""
    df = pd.read_excel(
        MAPPING_FILE,
        sheet_name=entity,
        dtype={"numero_compte": str}
    )
    df.columns = df.columns.str.strip()
    df["numero_compte"] = df["numero_compte"].astype(str).str.strip()
    return df


def load_all_mappings() -> dict[str, pd.DataFrame]:
    """Charge les mappings de toutes les entités."""
    mappings = {}
    for entity in ENTITIES:
        try:
            mappings[entity] = load_mapping(entity)
            print(f"✅ Mapping chargé : {entity}")
        except Exception as e:
            print(f"⚠️  Mapping {entity} : {e}")
    return mappings


def load_pl_structure() -> pd.DataFrame:
    """Charge la structure du P&L depuis l'onglet dédié."""
    df = pd.read_excel(MAPPING_FILE, sheet_name="Structure P&L")
    df.columns = df.columns.str.strip()
    return df


def load_split_ca_cogs(period: str = None) -> pd.DataFrame:
    """
    Charge le fichier de ventilation CA/COGS par BU.
    Si period fourni (format '01/MM/YYYY'), filtre sur cette période.
    """
    df = pd.read_excel(SPLIT_CA_COGS_FILE)
    df.columns = df.columns.str.strip()
    df["periode"] = pd.to_datetime(df["periode"], dayfirst=True)
    if period:
        # Convertit YYYYMM en datetime pour filtrer
        p = pd.to_datetime(period, format="%Y%m")
        df = df[df["periode"].dt.to_period("M") == p.to_period("M")]
    return df


def load_split_rh(period: str = None) -> pd.DataFrame:
    """
    Charge le fichier de ventilation RH (operating/non-operating + R&D).
    Si period fourni (format YYYYMM), filtre sur cette période.
    """
    df = pd.read_excel(SPLIT_RH_FILE)
    df.columns = df.columns.str.strip()
    df["periode"] = pd.to_datetime(df["periode"], dayfirst=True)
    if period:
        p = pd.to_datetime(period, format="%Y%m")
        df = df[df["periode"].dt.to_period("M") == p.to_period("M")]
    return df