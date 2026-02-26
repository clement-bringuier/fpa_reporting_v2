import pandas as pd


# ─────────────────────────────────────────────
# 1. CALCUL DU MONTANT NET PAR COMPTE
# ─────────────────────────────────────────────

def compute_net_by_account(fec: pd.DataFrame, period: str) -> pd.DataFrame:
    """
    Calcule le montant net par compte comptable sur le FEC pour le mois donné.
    Convention française :
    - Comptes de CHARGES (6x) : net = Debit - Credit
    - Comptes de PRODUITS (7x) : net = Credit - Debit
    On exclut les écritures d'à-nouveaux (JournalCode == 'AN')
    """
    p = pd.to_datetime(period, format="%Y%m")
    df = fec[
        (fec["JournalCode"] != "AN") &
        (fec["EcritureDate"].dt.year == p.year) &
        (fec["EcritureDate"].dt.month == p.month)
    ].copy()

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
    Retourne uniquement les comptes mappés sur le P&L.
    """
    df = net_by_account.merge(mapping, on="numero_compte", how="left")

    # Warning uniquement sur les comptes 6x et 7x non mappés
    unmapped = df[df["mapping_pl_detail"].isna() | (df["mapping_pl_detail"] == "NA")]
    unmapped_pl = unmapped[unmapped["numero_compte"].str.startswith(("6", "7"))]
    if not unmapped_pl.empty:
        print(f"⚠️  Comptes 6x/7x non mappés P&L : {unmapped_pl['numero_compte'].tolist()}")

    df = df[df["mapping_pl_detail"].notna() & (df["mapping_pl_detail"] != "NA")]

    # Mapping label P&L → code pour les lignes qui passent directement du FEC
    label_to_code = {
        "Furnitures": "i1",
        "Rents & other charges": "i2",
        "Maintenance & repairs + miscellaneous": "i4",
        "Insurances": "i5",
        "Accounting & audit fees": "i6",
        "Legal fees": "i7",
        "Postal charges": "i8",
        "Internet & telecom": "i9",
        "Banking fees": "i10",
        "Pro. Asso. Subscription": "i11",
        "Other Fees": "i12",
        "Internal events": "j1",
        "Exhibition & Events fees": "j2",
        "Accomodation & transport": "j3",
        "Reception": "j4",
        "Video / Image /Consulting  Providers": "f1",
        "Press & software subscriptions": "f2",
        "Miscellaneous": "f3",
        "Business provider fees": "e3",
        "China Office": "e4",
        "Freelance": "e1",
        "Server": "e2",
        "D&A on fixed assets": "m1",
        "D&A - Milestones": "m2",
    }
    df["mapping_pl_detail"] = df["mapping_pl_detail"].replace(label_to_code)
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
    """
    df = mapped.copy()
    split = split_ca_cogs[split_ca_cogs["entite"] == entity].copy()

    for pl_type, mapping_label in [("Revenue", "SALES"), ("COGS", "COGS")]:
        total_fec = df[df["mapping_pl_detail"] == mapping_label]["montant_net"].sum()
        print(f"  [DEBUG split BU] {entity} | {pl_type} | total_fec={total_fec}")

        if total_fec == 0:
            continue

        split_type = split[split["type"] == pl_type].copy()
        if split_type.empty:
            continue

        total_split = split_type["Montant"].sum()
        if total_split == 0:
            continue

        split_type["proportion"] = split_type["Montant"] / total_split
        split_type["montant_net"] = split_type["proportion"] * total_fec

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

        df = df[df["mapping_pl_detail"] != mapping_label]
        new_rows = split_type[["mapping_pl_detail", "mapping_pl_category", "montant_net"]].copy()
        df = pd.concat([df, new_rows], ignore_index=True)

    return df


# ─────────────────────────────────────────────
# 4. VENTILATION MASSE SALARIALE
# ─────────────────────────────────────────────

def split_staff_costs(
    mapped: pd.DataFrame,
    split_rh: pd.DataFrame,
    entity: str,
    total_ms_group: float = None
) -> pd.DataFrame:
    """
    Remplace les lignes 'Personnel costs to be allocated' par les lignes
    détaillées du fichier RH.
    Si total_ms_group est fourni, utilise ce montant comme base (total groupe FR+PID ou CELSIUS+VERTICAL).
    """
    df = mapped.copy()

    # Mapping entité → groupe RH
    rh_group = {
        "FR":       "PID+FR",
        "PID":      "PID+FR",
        "CELSIUS":  "CELSIUS+VERTICAL",
        "VERTICAL": "CELSIUS+VERTICAL",
    }
    group = rh_group.get(entity, entity)

    # Fichier RH pour ce groupe
    rh = split_rh[split_rh["entite"] == group].copy()
    if rh.empty:
        print(f"⚠️  Pas de données RH pour {entity} (groupe {group})")
        return df

    # Total masse salariale FEC de cette entité
    total_ms_entity = df[df["mapping_pl_detail"] == "Personnel costs to be allocated"]["montant_net"].sum()

    # Base de calcul : total groupe si fourni, sinon entité seule
    base = total_ms_group if total_ms_group is not None else total_ms_entity
    print(f"  [DEBUG RH] {entity} | total_ms_entity={total_ms_entity:.0f} | base={base:.0f} | lignes RH={len(rh)}")

    if base == 0:
        return df

    total_rh = rh["montant"].sum()
    if total_rh == 0:
        return df

    rh = rh.copy()

    # Lignes de staff costs (prorata sur MS FEC)
    staff_lines = ["Operating staff costs", "Non-operating staff costs"]
    rh_staff = rh[rh["type"].isin(staff_lines)].copy()
    total_staff_rh = rh_staff["montant"].sum()

    # Lignes d'activation (montant direct du fichier, pas de prorata)
    activation_lines = ["Operating activation", "Activation Liveops", "Activation Internal Projects", "CIJV", "Non-operating activation"]
    rh_activations = rh[rh["type"].isin(activation_lines)].copy()

    # Calcul des montants
    if total_staff_rh > 0:
        rh_staff["proportion"] = rh_staff["montant"] / total_staff_rh
        rh_staff["montant_net"] = rh_staff["proportion"] * total_ms_entity
    else:
        rh_staff["montant_net"] = 0.0

    # Activations : prorata MS entité / MS groupe
    ratio_entity = total_ms_entity / base if base > 0 else 0
    rh_activations["montant_net"] = rh_activations["montant"] * ratio_entity

    # Dotation m3 : somme des activations de cette entité en négatif
    total_activations = rh_activations["montant_net"].sum()
    da_hr = pd.DataFrame([{
        "type": "D&A on HR",
        "montant_net": -total_activations
    }])

    rh_to_pl = {
        "Operating staff costs":        "d1",
        "Operating activation":          "d2",
        "Activation Liveops":            "d3",
        "Activation Internal Projects":  "d4",
        "CIJV":                          "d5",
        "Non-operating staff costs":     "h1",
        "Non-operating activation":      "h2",
        "D&A on HR":                     "m3",
    }

    rh_staff["mapping_pl_detail"] = rh_staff["type"].map(rh_to_pl)
    rh_staff["mapping_pl_category"] = rh_staff["type"].apply(
        lambda t: "Operating staff costs" if t == "Operating staff costs" else "Non-operating staff costs"
    )
    rh_activations["mapping_pl_detail"] = rh_activations["type"].map(rh_to_pl)
    rh_activations["mapping_pl_category"] = rh_activations["type"].apply(
        lambda t: "Operating staff costs" if t in [
            "Operating activation", "Activation Liveops", "Activation Internal Projects", "CIJV"
        ] else "Non-operating staff costs"
    )
    da_hr["mapping_pl_detail"] = "m3"
    da_hr["mapping_pl_category"] = "D&A"

    df = df[df["mapping_pl_detail"] != "Personnel costs to be allocated"]
    new_rows = pd.concat([
        rh_staff[["mapping_pl_detail", "mapping_pl_category", "montant_net"]],
        rh_activations[["mapping_pl_detail", "mapping_pl_category", "montant_net"]],
        da_hr[["mapping_pl_detail", "mapping_pl_category", "montant_net"]],
    ], ignore_index=True)
    df = pd.concat([df, new_rows], ignore_index=True)

    return df


# ─────────────────────────────────────────────
# 5. RETRAITEMENT IFRS 16
# ─────────────────────────────────────────────

def apply_ifrs16(mapped: pd.DataFrame) -> pd.DataFrame:
    """
    Retraitement IFRS 16 :
    - Ajoute i3 IFRS 16 activation en négatif (annule les loyers)
    - Ajoute m4 D&A on IFRS 16 en positif (charge de dotation)
    """
    df = mapped.copy()
    loyers = df[df["mapping_pl_detail"] == "Rents & other charges"]["montant_net"].sum()

    if loyers == 0:
        return df

    ifrs16_activation = pd.DataFrame([{
        "mapping_pl_detail": "IFRS 16 activation",
        "mapping_pl_category": "Structure costs",
        "montant_net": -loyers
    }])

    ifrs16_da = pd.DataFrame([{
        "mapping_pl_detail": "D&A on IFRS 16",
        "mapping_pl_category": "D&A",
        "montant_net": loyers
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
    entity: str,
    period: str,
    total_ms_group: float = None
) -> pd.DataFrame:
    """
    Pipeline complet de transformation pour une entité.
    """
    # 1. Calcul montants nets par compte (filtré sur le mois)
    net = compute_net_by_account(fec, period)

    # 2. Application du mapping
    mapped = apply_mapping(net, mapping)

    # 3. Ventilation CA/COGS par BU
    mapped = split_revenue_cogs_by_bu(mapped, split_ca_cogs, entity)

    # 4. Ventilation masse salariale
    mapped = split_staff_costs(mapped, split_rh, entity, total_ms_group)

    # 5. Retraitement IFRS 16
    mapped = apply_ifrs16(mapped)

    # Agrégation finale par ligne P&L
    result = (
        mapped.groupby(["mapping_pl_detail", "mapping_pl_category"], as_index=False)["montant_net"]
        .sum()
    )
    result["entite"] = entity

    return result