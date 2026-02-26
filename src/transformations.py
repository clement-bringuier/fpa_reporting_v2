import pandas as pd

RH_GROUP = {
    "FR":       "PID+FR",
    "PID":      "PID+FR",
    "CELSIUS":  "CELSIUS+VERTICAL",
    "VERTICAL": "CELSIUS+VERTICAL",
}

REVENUE_BU_MAP = {
    "Publishing": "a1", "Distribution": "a2",
    "RR": "a3", "MGG": "a4", "Autres B2C": "a5", "B2B": "a6",
}

COGS_BU_MAP = {
    "Publishing": "b1", "Distribution": "b2", "Celsius": "b3",
}

RH_TO_PL = {
    "Operating staff costs":       "d1",
    "Operating activation":        "d2",
    "Activation Liveops":          "d3",
    "Activation Internal Projects":"d4",
    "CIJV":                        "d5",
    "Non-operating staff costs":   "h1",
    "Non-operating activation":    "h2",
}

STAFF_TYPES = ["Operating staff costs", "Non-operating staff costs"]
ACTIVATION_TYPES = [
    "Operating activation", "Activation Liveops",
    "Activation Internal Projects", "CIJV", "Non-operating activation"
]


def compute_net_by_account(fec: pd.DataFrame, period: str) -> pd.DataFrame:
    p = pd.to_datetime(period, format="%Y%m")
    df = fec[
        (fec["JournalCode"] != "AN") &
        (fec["EcritureDate"].dt.year == p.year) &
        (fec["EcritureDate"].dt.month == p.month)
    ].copy()
    df["montant_net"] = df.apply(
        lambda r: r["Credit"] - r["Debit"] if r["CompteNum"][0] == "7"
                  else r["Debit"] - r["Credit"],
        axis=1
    )
    return (
        df.groupby("CompteNum", as_index=False)["montant_net"]
        .sum()
        .rename(columns={"CompteNum": "numero_compte"})
    )


def apply_mapping(net: pd.DataFrame, mapping: pd.DataFrame) -> pd.DataFrame:
    EXCLUDE = {"NA", "below_ebit", "management_fees"}
    df = net.merge(mapping, on="numero_compte", how="left")
    df["mapping_pl_detail"] = df["mapping_pl_detail"].fillna("NA")
    unmapped = df[
        (df["mapping_pl_detail"] == "NA") &
        df["numero_compte"].str.startswith(("6", "7"))
    ]
    if not unmapped.empty:
        print(f"    ⚠️  Comptes 6x/7x sans mapping : {unmapped['numero_compte'].tolist()}")
    df = df[~df["mapping_pl_detail"].isin(EXCLUDE)]
    return df[["numero_compte", "mapping_pl_detail", "montant_net"]]


def split_revenue_cogs(mapped: pd.DataFrame, split_ca: pd.DataFrame, entity: str) -> pd.DataFrame:
    df = mapped.copy()
    split = split_ca[split_ca["entite"] == entity].copy()

    for fec_code, split_type, bu_map in [
        ("rev_to_allocate",  "Revenue", REVENUE_BU_MAP),
        ("cogs_to_allocate", "COGS",    COGS_BU_MAP),
    ]:
        total_fec = df[df["mapping_pl_detail"] == fec_code]["montant_net"].sum()
        if total_fec == 0:
            df = df[df["mapping_pl_detail"] != fec_code]
            continue

        split_t = split[split["type"] == split_type].copy()
        if split_t.empty:
            print(f"    ⚠️  Pas de split {split_type} pour {entity}")
            df = df[df["mapping_pl_detail"] != fec_code]
            continue

        total_split = split_t["Montant"].sum()
        if total_split == 0:
            df = df[df["mapping_pl_detail"] != fec_code]
            continue

        split_t["proportion"]        = split_t["Montant"] / total_split
        split_t["montant_net"]       = split_t["proportion"] * total_fec
        split_t["mapping_pl_detail"] = split_t["BU"].map(bu_map)

        df = df[df["mapping_pl_detail"] != fec_code]
        new_rows = split_t[["mapping_pl_detail", "montant_net"]].copy()
        new_rows["numero_compte"] = fec_code
        df = pd.concat([df, new_rows], ignore_index=True)

    return df


def split_staff_costs(
    mapped: pd.DataFrame,
    split_rh: pd.DataFrame,
    entity: str,
    total_ms_group: float
) -> pd.DataFrame:
    df = mapped.copy()
    total_ms_entity = df[df["mapping_pl_detail"] == "rh_to_allocate"]["montant_net"].sum()
    ratio = total_ms_entity / total_ms_group if total_ms_group > 0 else 0

    group = RH_GROUP[entity]
    rh = split_rh[split_rh["entite"] == group].copy()
    if rh.empty:
        print(f"    ⚠️  Pas de données RH pour groupe {group}")
        return df

    rh_staff_lines = rh[rh["type"].isin(STAFF_TYPES)].copy()
    rh_activ_lines = rh[rh["type"].isin(ACTIVATION_TYPES)].copy()
    total_staff_rh = rh_staff_lines["montant"].sum()

    rows = []

    for _, r in rh_staff_lines.iterrows():
        prop = r["montant"] / total_staff_rh if total_staff_rh > 0 else 0
        rows.append({
            "numero_compte":     "rh_to_allocate",
            "mapping_pl_detail": RH_TO_PL[r["type"]],
            "montant_net":       prop * total_ms_entity
        })

    total_activations_entity = 0
    for _, r in rh_activ_lines.iterrows():
        montant = r["montant"] * ratio
        total_activations_entity += montant
        rows.append({
            "numero_compte":     "rh_to_allocate",
            "mapping_pl_detail": RH_TO_PL.get(r["type"], r["type"]),
            "montant_net":       montant
        })

    if total_activations_entity > 0:
        rows.append({
            "numero_compte":     "rh_to_allocate",
            "mapping_pl_detail": "m3",
            "montant_net":       -total_activations_entity
        })

    df = df[df["mapping_pl_detail"] != "rh_to_allocate"]
    df = pd.concat([df, pd.DataFrame(rows)], ignore_index=True)
    return df


def apply_ifrs16(mapped: pd.DataFrame) -> pd.DataFrame:
    df = mapped.copy()
    loyers = df[df["mapping_pl_detail"] == "i2_ifrs16"]["montant_net"].sum()
    if loyers == 0:
        return df
    # Requalifie i2_ifrs16 en i2 (reste dans Rents & other charges)
    df.loc[df["mapping_pl_detail"] == "i2_ifrs16", "mapping_pl_detail"] = "i2"
    # Ajoute activation i3 et dotation m4
    ifrs16 = pd.DataFrame([
        {"numero_compte": "ifrs16", "mapping_pl_detail": "i3", "montant_net":  loyers},
        {"numero_compte": "ifrs16", "mapping_pl_detail": "m4", "montant_net":  loyers},
    ])
    return pd.concat([df, ifrs16], ignore_index=True)


def aggregate(mapped: pd.DataFrame, entity: str) -> pd.DataFrame:
    result = (
        mapped.groupby("mapping_pl_detail", as_index=False)["montant_net"]
        .sum()
    )
    result["entite"] = entity
    return result


def transform_entity(
    fec: pd.DataFrame,
    mapping: pd.DataFrame,
    split_ca: pd.DataFrame,
    split_rh: pd.DataFrame,
    entity: str,
    period: str,
    total_ms_group: float
) -> pd.DataFrame:
    net    = compute_net_by_account(fec, period)
    mapped = apply_mapping(net, mapping)
    mapped = split_revenue_cogs(mapped, split_ca, entity)
    mapped = split_staff_costs(mapped, split_rh, entity, total_ms_group)
    mapped = apply_ifrs16(mapped)
    return aggregate(mapped, entity)