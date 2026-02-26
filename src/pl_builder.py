import pandas as pd


# ─────────────────────────────────────────────
# CHARGES EN NÉGATIF
# ─────────────────────────────────────────────

CHARGE_CODES = {
    "b1", "b2", "b3",
    "d1",
    "e1", "e2", "e3", "e4",
    "f1", "f2", "f3",
    "h1",
    "i1", "i2", "i4", "i5", "i6", "i7", "i8", "i9", "i10", "i11", "i12",
    "j1", "j2", "j3", "j4",
    "m1", "m2", "m4",
}


# ─────────────────────────────────────────────
# CONSTRUCTION DU TOTALS_MAP DEPUIS LA STRUCTURE
# ─────────────────────────────────────────────

def build_totals_map(structure: list) -> dict:
    """
    Construit le TOTALS_MAP dynamiquement depuis la structure P&L.
    Pour chaque ligne total/margin, parse les codes composants depuis la formule.
    Ex: "a1+a2+a3" → ["a1", "a2", "a3"]
    
    On mappe les codes composants vers les codes des totaux intermédiaires
    en utilisant un dictionnaire code_detail → code_total déjà vu.
    """
    # D'abord construire un mapping formule → code total pour les totaux déjà vus
    formula_to_code = {}
    detail_codes = set()

    for code, label, row_type in structure:
        if row_type == "detail":
            detail_codes.add(code)
        else:
            formula_to_code[code] = code  # le code EST la formule ici

    totals_map = {}
    # Pour résoudre les totaux de totaux, on garde un mapping
    # formule_complète → code interne
    seen_totals = {}  # label → code interne

    for code, label, row_type in structure:
        if row_type in ("total", "margin"):
            # Parse les composants depuis le code (formule)
            raw_components = code.split("+")
            # Chaque composant est soit un code simple (a1) soit une sous-formule
            # On résout : si c'est un code detail → direct
            # Si c'est une formule connue → on cherche le code interne du total
            resolved = []
            for comp in raw_components:
                comp = comp.strip()
                if comp in detail_codes:
                    resolved.append(comp)
                else:
                    # Cherche dans les totaux déjà vus par leur formule
                    if comp in seen_totals:
                        resolved.append(seen_totals[comp])
                    else:
                        resolved.append(comp)
            totals_map[label] = resolved
            seen_totals[code] = code

    return totals_map


# ─────────────────────────────────────────────
# CONSTRUCTION DU P&L PAR ENTITÉ
# ─────────────────────────────────────────────

def build_pl_entity(transformed: pd.DataFrame, entity: str, structure: list, totals_map: dict) -> pd.Series:
    """
    Construit le P&L d'une entité pour une structure donnée.
    """
    data = dict(zip(transformed["mapping_pl_detail"], transformed["montant_net"]))

    pl = {}
    code_to_value = {}

    for code, label, row_type in structure:
        if row_type == "detail":
            val = data.get(code, 0.0)
            if code in CHARGE_CODES:
                val = -abs(val)
            pl[label] = val
            code_to_value[code] = val
        elif row_type in ("total", "margin"):
            components = totals_map.get(label, [])
            val = sum(code_to_value.get(c, 0.0) for c in components)
            pl[label] = val
            code_to_value[code] = val

    return pd.Series(pl, name=entity)


# ─────────────────────────────────────────────
# CONSOLIDATION
# ─────────────────────────────────────────────

def build_consolidated_pl(entity_series: list, structure: list, totals_map: dict) -> pd.DataFrame:
    """
    Consolide les P&L de plusieurs entités.
    """
    df = pd.concat(entity_series, axis=1)
    df["Total"] = df.sum(axis=1)

    # Recalcule totaux et marges sur la colonne Total
    code_to_value = {}
    for code, label, row_type in structure:
        if row_type == "detail":
            code_to_value[code] = df.loc[label, "Total"] if label in df.index else 0.0
        elif row_type in ("total", "margin"):
            components = totals_map.get(label, [])
            val = sum(code_to_value.get(c, 0.0) for c in components)
            if label in df.index:
                df.loc[label, "Total"] = val
            code_to_value[code] = val

    return df


# ─────────────────────────────────────────────
# PIPELINE COMPLET
# ─────────────────────────────────────────────

def build_full_pl(transformed_by_entity: dict, pl_structures: dict) -> dict:
    """
    Construit les 3 P&L consolidés depuis les structures dynamiques.
    """
    groups = {
        "P&L PID":       ["FR", "PID"],
        "P&L Celsius":   ["CELSIUS", "VERTICAL"],
        "P&L Consolidé": ["FR", "PID", "CELSIUS", "VERTICAL"],
    }

    result = {}
    for sheet_name, entities in groups.items():
        structure = pl_structures[sheet_name]
        totals_map = build_totals_map(structure)

        # Labels ordonnés sans doublons
        seen = set()
        ordered_labels = []
        for _, label, _ in structure:
            if label not in seen:
                ordered_labels.append(label)
                seen.add(label)

        series = []
        for entity in entities:
            if entity in transformed_by_entity:
                s = build_pl_entity(transformed_by_entity[entity], entity, structure, totals_map)
                series.append(s)

        if not series:
            continue

        pl = build_consolidated_pl(series, structure, totals_map)
        pl = pl[["Total"]].rename(columns={"Total": "Montant"})
        pl = pl.reindex(ordered_labels)
        result[sheet_name] = pl

    return result