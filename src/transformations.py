import pandas as pd


# ─────────────────────────────────────────────
# 1. CALCUL DU MONTANT NET PAR COMPTE
# ─────────────────────────────────────────────

def compute_net_by_account(fec: pd.DataFrame) -> pd.DataFrame:
    """
    Calcule le montant net par compte comptable sur le FEC.
    
    Convention française :
    - Comptes de CHARGES (6x) : net = Debit - Credit  → positif = charge
    - Comptes de PRODUITS (7x) : net = Credit - Debit  → positif = produit
    - Autres comptes (bilan) : net = Debit - Credit
    
    On exclut les écritures d'à-nouveaux (JournalCode == 'AN')
    """
    df = fec[fec["JournalCode"] != "AN"].copy()

    df["compte_classe"] = df["CompteNum"].str[0]

    df["montant_net"] = df.apply(
        lambda r: (r["Credit"] - r["Debit"]) if r["compte_classe"] == "7"
        else (r["Debit"] - r["Credit"]),
        axis=1
    )

    result = (
        df.groupby("CompteNum", as_index=False)["montant_net"]
        .sum()
        .rename(columns={"CompteNum": "numero_compte"})
    )
    return result


# ─────────────────────────────────────────────
# 2. MERGE AVEC LE MAPPING
# ─────────────────────────────────────────────

def apply_mapping(net_by_account: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    """
    Joint les montants nets avec le mapping PCG.
    Retourne uniquement les comptes mappés sur le P&L (mapping_pl_detail != 'NA').
    """
    df = net_by_account.merge(mapping, on="numero_compte", how="left")

    # Comptes non mappés → warning
    unmapped = df[df["mapping_pl_detail"].isna() | (df["mapping_pl_detail"] == "NA")]
    if not unmapped.empty:
        unmapped_accounts = unmapped["numero_compte"].tolist()
        print(f"⚠️  Comptes non mappés P&L : {unmapped_accounts}")

    # Garde uniquement les comptes P&L
    df = df[df["mapping_pl_detail"].notna() & (df["mapping_pl_detail"] != "NA")]
    return df


# ─────────────────────────────────────────────
# 3. VENTILATION CA / COGS PAR BU
# ─────────────────────────────────────────────

def split_revenue_cogs_by_bu(
    mapped: pd.DataFrame,
    split_ca_cogs: pd.DataFrame,
    entity: str
) -> pd.DataFrame:
    """
    Remplace les lignes SALES et COGS du FEC par une ventilation par BU.
    
    Logique :
    - Calcule le total FEC pour Revenue et COGS
    - Calcule les proportions BU depuis split_ca_cogs
    - Génère une ligne par BU avec montant = proportion × total FEC
    """
    df = mapped.copy()

    # Filtre entité dans le fichier split
    split = split_ca_cogs[split_ca_cogs["entite"] == entity].copy()

    for pl_type, mapping_label in [("Revenue", "SALES"), ("COGS", "COGS")]:
        # Total FEC pour ce type
        total_fec = df[df["mapping_pl_detail"] == mapping_label]["montant_net"].sum()

        if total_fec == 0:
            continue

        # Proportions BU
        split_type = split[split["type"] == pl_type].copy()
        if split_type.empty:
            continue

        total_split = split_type["Montant"].sum()
        if total_split == 0:
            continue

        split_type["proportion"] = split_type["Montant"] / total_split
        split_type["montant_net"] = split_type["proportion"] * total_fec

        # Mapping BU → code ligne P&L
        bu_to_pl = {
            "Publishing": "a1", "Distribution": "a2", "RR": "a3",
            "MGG": "a4", "Autres B2C": "a5", "B2B": "a6",  # Revenue
            "Publishing": "b1", "Distribution": "b2", "Celsius": "b3",  # COGS
        }
        # Mapping séparé pour éviter collision
        if pl_type == "Revenue":
            bu_map = {
                "Publishing": "a1", "Distribution": "a2", "RR": "a3",
                "MGG": "a4", "Autres B2C": "a5", "B2B": "a6"
            }
        else:
            bu_map = {
                "Publishing": "b1", "Distribution": "b2", "Celsius": "b3"
            }

        split_type["mapping_pl_detail"] = split_type["BU"].map(bu_map)
        split_type["mapping_pl_category"] = pl_type

        # Supprime les lignes SALES/COGS globales du df
        df = df[df["mapping_pl_detail"] != mapping_label]

        # Ajoute les lignes ventilées
        new_rows = split_type[["mapping_pl_detail", "mapping_pl_category", "montant_net"]].copy()
        df = pd.concat([df, new_rows], ignore_index=True)

    return df


# ─────────────────────────────────────────────
# 4. VENTILATION MASSE SALARIALE
# ─────────────────────────────────────────────

def split_staff_costs(
    mapped: pd.DataFrame,
    split_rh: pd.DataFrame,
    entity: str
) -> pd.DataFrame:
    """
    Remplace les lignes 'Personnel costs to be allocated' par les lignes
    détaillées du fichier RH (operating, non-operating, activations R&D).
    
    Logique :
    - Total masse salariale FEC = somme des comptes 'Personnel costs to be allocated'
    - Proportions depuis split_rh
    - Génère une ligne par type RH
    """
    df = mapped.copy()

    # Total masse salariale FEC
    total_ms = df[df["mapping_pl_detail"] == "Personnel costs to be allocated"]["montant_net"].sum()

    if total_ms == 0:
        print(f"⚠️  Masse salariale FEC nulle pour {entity}")
        return df

    # Fichier RH pour cette entité
    rh = split_rh[split_rh["entite"] == entity].copy()
    if rh.empty:
        print(f"⚠️  Pas de données RH pour {entity}")
        return df

    total_rh = rh["montant"].sum()
    if total_rh == 0:
        return df

    rh["proportion"] = rh["montant"] / total_rh
    rh["montant_net"] = rh["proportion"] * total_ms

    # Mapping type RH → code ligne P&L
    rh_to_pl = {
        "Operating staff costs": "d1",
        "Operating activation": "d2",
        "Activation Liveops": "d3",
        "Activation Internal Projects": "d4",
        "CIJV": "d5",
        "Non-operating staff costs": "h1",
        "Non-operating activation": "h2",
    }

    rh["mapping_pl_detail"] = rh["type"].map(rh_to_pl)
    rh["mapping_pl_category"] = rh["type"].apply(
        lambda t: "Operating staff costs" if t in ["Operating staff costs", "Operating activation",
                                                    "Activation Liveops", "Activation Internal Projects", "CIJV"]
        else "Non-operating staff costs"
    )

    # Supprime les lignes masse salariale globales
    df = df[df["mapping_pl_detail"] != "Personnel costs to be allocated"]

    # Ajoute les lignes ventilées
    new_rows = rh[["mapping_pl_detail", "mapping_pl_category", "montant_net"]].copy()
    df = pd.concat([df, new_rows], ignore_index=True)

    return df


# ─────────────────────────────────────────────
# 5. RETRAITEMENT IFRS 16 (COÛTS DE LOCATION)
# ─────────────────────────────────────────────

def apply_ifrs16(mapped: pd.DataFrame) -> pd.DataFrame:
    """
    Retraitement IFRS 16 :
    - Identifie les comptes de loyers (mapping_pl_detail == 'Rents & other charges')
    - Ajoute une ligne d'activation IFRS 16 en positif (i3) = montant des loyers
    - Ajoute une ligne de dotation IFRS 16 en négatif (m4) = -montant des loyers
    
    Les loyers restent dans i2 (Rents & other charges) mais sont annulés
    par l'activation i3, et la dotation m4 reprend le même montant.
    """
    df = mapped.copy()

    # Montant total des loyers
    loyers = df[df["mapping_pl_detail"] == "Rents & other charges"]["montant_net"].sum()

    if loyers == 0:
        return df

    # Ligne i3 : IFRS 16 activation (en négatif car réduit les charges)
    ifrs16_activation = pd.DataFrame([{
        "mapping_pl_detail": "IFRS 16 activation",
        "mapping_pl_category": "Structure costs",
        "montant_net": -loyers  # annule les loyers dans les charges
    }])

    # Ligne m4 : D&A on IFRS 16 (en positif car charge de dotation)
    ifrs16_da = pd.DataFrame([{
        "mapping_pl_detail": "D&A on IFRS 16",
        "mapping_pl_category": "D&A",
        "montant_net": loyers  # remet la charge en dotation
    }])

    df = pd.concat([df, ifrs16_activation, ifrs16_da], ignore_index=True)
    return df


# ─────────────────────────────────────────────
# 6. PIPELINE COMPLET PAR ENTITÉ
# ─────────────────────────────────────────────

def transform_entity(
    fec: pd.DataFrame,
    mapping: pd.DataFrame,
    split_ca_cogs: pd.DataFrame,
    split_rh: pd.DataFrame,
    entity: str
) -> pd.DataFrame:
    """
    Pipeline complet de transformation pour une entité.
    Retourne un DataFrame avec les colonnes :
    mapping_pl_detail, mapping_pl_category, montant_net
    """
    # 1. Calcul montants nets par compte
    net = compute_net_by_account(fec)

    # 2. Application du mapping
    mapped = apply_mapping(net, mapping)

    # 3. Ventilation CA/COGS par BU
    mapped = split_revenue_cogs_by_bu(mapped, split_ca_cogs, entity)

    # 4. Ventilation masse salariale
    mapped = split_staff_costs(mapped, split_rh, entity)

    # 5. Retraitement IFRS 16
    mapped = apply_ifrs16(mapped)

    # Agrégation finale par ligne P&L
    result = (
        mapped.groupby(["mapping_pl_detail", "mapping_pl_category"], as_index=False)["montant_net"]
        .sum()
    )
    result["entite"] = entity

    return result