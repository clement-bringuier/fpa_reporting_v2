import pandas as pd


# ─────────────────────────────────────────────
# STRUCTURE P&L
# ─────────────────────────────────────────────

# Définition complète de la structure du P&L
# (code, label, type) où type = 'detail', 'total', ou 'margin'
PL_STRUCTURE = [
    # REVENUE
    ("a1", "Publishing",                "detail"),
    ("a2", "Distribution",              "detail"),
    ("a3", "RR",                        "detail"),
    ("a4", "MGG",                       "detail"),
    ("a5", "Autres B2C",                "detail"),
    ("a6", "B2B",                       "detail"),
    ("Revenue", "Revenue",              "total"),

    # COGS
    ("b1", "Publishing",                "detail"),
    ("b2", "Distribution",              "detail"),
    ("b3", "Celsius",                   "detail"),
    ("COGS", "COGS",                    "total"),

    # GROSS MARGIN
    ("c1", "Gross Margin",              "margin"),

    # OPERATING STAFF COSTS
    ("d1", "Operating staff costs",     "detail"),
    ("d2", "Operating activation",      "detail"),
    ("d3", "Activation Liveops",        "detail"),
    ("d4", "Activation Internal Projects", "detail"),
    ("d5", "CIJV",                      "detail"),
    ("Opex Staff", "Operating staff costs", "total"),

    # OUTSOURCING
    ("e1", "Freelance",                 "detail"),
    ("e2", "Server",                    "detail"),
    ("e3", "Business provider fees",    "detail"),
    ("e4", "China Office",              "detail"),
    ("Outsourcing", "Outsourcing costs","total"),

    # MARKETING & OTHER OPEX
    ("f1", "Video / Image /Consulting  Providers", "detail"),
    ("f2", "Press & software subscriptions",       "detail"),
    ("f3", "Miscellaneous",             "detail"),
    ("Marketing", "Marketing & Other Opex", "total"),

    # CONTRIBUTION MARGIN
    ("g1", "Contribution Margin",       "margin"),

    # NON-OPERATING STAFF COSTS
    ("h1", "Non-operating staff costs", "detail"),
    ("h2", "Non-operating activation",  "detail"),
    ("Non-op Staff", "Non-operating staff costs", "total"),

    # STRUCTURE COSTS
    ("i1",  "Furnitures",               "detail"),
    ("i2",  "Rents & other charges",    "detail"),
    ("i3",  "IFRS 16 activation",       "detail"),
    ("i4",  "Maintenance & repairs + miscellaneous", "detail"),
    ("i5",  "Insurances",               "detail"),
    ("i6",  "Accounting & audit fees",  "detail"),
    ("i7",  "Legal fees",               "detail"),
    ("i8",  "Postal charges",           "detail"),
    ("i9",  "Internet & telecom",       "detail"),
    ("i10", "Banking fees",             "detail"),
    ("i11", "Pro. Asso. Subscription",  "detail"),
    ("i12", "Other Fees",               "detail"),
    ("Structure", "Structure costs",    "total"),

    # EVENTS
    ("j1", "Internal events",           "detail"),
    ("j2", "Exhibition & Events fees",  "detail"),
    ("j3", "Accomodation & transport",  "detail"),
    ("j4", "Reception",                 "detail"),
    ("Events", "Events, accomodation and transport", "total"),

    # NON OPERATING COSTS
    ("k1", "Non operating costs",       "margin"),

    # EBITDA
    ("l1", "EBITDA",                    "margin"),

    # D&A
    ("m1", "D&A on fixed assets",       "detail"),
    ("m2", "D&A - Milestones",          "detail"),
    ("m3", "D&A on HR",                 "detail"),
    ("m4", "D&A on IFRS 16",            "detail"),
    ("DA",  "D&A",                      "total"),

    # EBIT
    ("n1", "EBIT",                      "margin"),
]

# Mapping label P&L → code pour les totaux et marges
TOTALS_MAP = {
    "Revenue":      ["a1", "a2", "a3", "a4", "a5", "a6"],
    "COGS":         ["b1", "b2", "b3"],
    "Gross Margin": ["Revenue", "COGS"],        # Revenue - COGS (COGS déjà en négatif)
    "Operating staff costs": ["d1", "d2", "d3", "d4", "d5"],
    "Outsourcing costs":     ["e1", "e2", "e3", "e4"],
    "Marketing & Other Opex":["f1", "f2", "f3"],
    "Contribution Margin":   ["Revenue", "COGS", "Opex Staff", "Outsourcing", "Marketing"],
    "Non-operating staff costs": ["h1", "h2"],
    "Structure costs":       ["i1", "i2", "i3", "i4", "i5", "i6", "i7", "i8", "i9", "i10", "i11", "i12"],
    "Events, accomodation and transport": ["j1", "j2", "j3", "j4"],
    "Non operating costs":   ["Non-op Staff", "Structure", "Events"],
    "EBITDA":                ["g1", "k1"],      # Contribution Margin + Non operating costs
    "D&A":                   ["m1", "m2", "m3", "m4"],
    "EBIT":                  ["l1", "DA"],      # EBITDA + D&A
}


# ─────────────────────────────────────────────
# CONSTRUCTION DU P&L PAR ENTITÉ
# ─────────────────────────────────────────────

def build_pl_entity(transformed: pd.DataFrame, entity: str) -> pd.Series:
    """
    Construit le P&L d'une entité depuis les données transformées.
    Retourne une Series indexée par les labels du P&L.
    """
    # Dictionnaire label → montant depuis les données transformées
    data = dict(zip(transformed["mapping_pl_detail"], transformed["montant_net"]))

    pl = {}

    # Codes de référence pour les calculs de totaux
    code_to_value = {}

    for code, label, row_type in PL_STRUCTURE:
        if row_type == "detail":
            val = data.get(label, 0.0)
            pl[label] = val
            code_to_value[code] = val

        elif row_type == "total":
            components = TOTALS_MAP.get(label, [])
            val = sum(code_to_value.get(c, 0.0) for c in components)
            pl[label] = val
            code_to_value[code] = val

        elif row_type == "margin":
            components = TOTALS_MAP.get(label, [])
            val = sum(code_to_value.get(c, 0.0) for c in components)
            pl[label] = val
            code_to_value[code] = val

    return pd.Series(pl, name=entity)


# ─────────────────────────────────────────────
# CONSOLIDATION
# ─────────────────────────────────────────────

def build_consolidated_pl(entity_series: list[pd.Series]) -> pd.DataFrame:
    """
    Consolide les P&L de toutes les entités.
    Retourne un DataFrame avec une colonne par entité + une colonne Total.
    """
    df = pd.concat(entity_series, axis=1)

    # Lignes de détail → somme simple
    # Lignes de total/marge → recalculées sur le consolidé pour être propres
    # On recalcule le consolidé comme une entité supplémentaire
    total_data = df.sum(axis=1)
    total_series = total_data.rename("Total")

    # Recalcule les totaux et marges sur le consolidé
    # (évite les doubles comptages sur les lignes calculées)
    detail_labels = {label for code, label, t in PL_STRUCTURE if t == "detail"}
    for code, label, row_type in PL_STRUCTURE:
        if row_type in ("total", "margin"):
            components = TOTALS_MAP.get(label, [])
            total_series[label] = sum(
                total_series.get(c, 0.0) if isinstance(total_series, dict)
                else total_series[c] if c in total_series.index else 0.0
                for c in [l for _, l, _ in PL_STRUCTURE if _ in ("detail",) and _ in components]
            )

    df["Total"] = df.sum(axis=1)

    # Recalcule proprement les totaux et marges sur la colonne Total
    code_to_idx = {code: label for code, label, _ in PL_STRUCTURE}
    code_to_value = {}

    for code, label, row_type in PL_STRUCTURE:
        if row_type == "detail":
            code_to_value[code] = df.loc[label, "Total"] if label in df.index else 0.0
        elif row_type in ("total", "margin"):
            components = TOTALS_MAP.get(label, [])
            val = sum(code_to_value.get(c, 0.0) for c in components)
            df.loc[label, "Total"] = val
            code_to_value[code] = val

    return df


# ─────────────────────────────────────────────
# PIPELINE COMPLET
# ─────────────────────────────────────────────

def build_full_pl(transformed_by_entity: dict[str, pd.DataFrame]) -> dict[str, pd.DataFrame]:
    """
    Construit les 3 P&L consolidés depuis les données transformées de chaque entité.

    Returns:
        dict avec 3 clés :
        - "P&L PID"        : consolidé FR + PID
        - "P&L Celsius"    : consolidé CELSIUS + VERTICAL
        - "P&L Consolidé"  : consolidé total
    """
    groups = {
        "P&L PID":       ["FR", "PID"],
        "P&L Celsius":   ["CELSIUS", "VERTICAL"],
        "P&L Consolidé": ["FR", "PID", "CELSIUS", "VERTICAL"],
    }

    # Réordonne les labels selon la structure (dédupliqué)
    seen = set()
    ordered_labels = []
    for _, label, _ in PL_STRUCTURE:
        if label not in seen:
            ordered_labels.append(label)
            seen.add(label)

    result = {}
    for sheet_name, entities in groups.items():
        series = []
        for entity in entities:
            if entity in transformed_by_entity:
                s = build_pl_entity(transformed_by_entity[entity], entity)
                series.append(s)

        if not series:
            continue

        pl = build_consolidated_pl(series)

        # Garde uniquement la colonne Total et la renomme "Montant"
        pl = pl[["Total"]].rename(columns={"Total": "Montant"})
        pl = pl.reindex(ordered_labels)

        result[sheet_name] = pl

    return result