import sys
import pandas as pd

from src.loaders import (
    load_all_fec,
    load_all_mappings,
    load_all_pl_structures,
    load_split_ca_cogs,
    load_split_rh,
)
from src.transformations import (
    compute_net_by_account,
    apply_mapping,
    transform_entity,
)
from src.pl_builder import build_full_pl
from src.output import export_to_excel
from src.controls import run_all_controls


# ─────────────────────────────────────────────
# GROUPES
# ─────────────────────────────────────────────

RH_GROUPS = {
    "PID+FR":           ["FR", "PID"],
    "CELSIUS+VERTICAL": ["CELSIUS", "VERTICAL"],
}

ENTITY_TO_RH_GROUP = {
    "FR":       "PID+FR",
    "PID":      "PID+FR",
    "CELSIUS":  "CELSIUS+VERTICAL",
    "VERTICAL": "CELSIUS+VERTICAL",
}


# ─────────────────────────────────────────────
# PIPELINE
# ─────────────────────────────────────────────

def run(period: str):
    print(f"\n{'='*52}")
    print(f"  GENERATION P&L — {period}")
    print(f"{'='*52}\n")

    # ── 1. Chargement ────────────────────────────────
    print("📂 Chargement des fichiers...\n")
    fecs          = load_all_fec(period)
    mappings      = load_all_mappings()
    pl_structures = load_all_pl_structures()
    split_ca      = load_split_ca_cogs(period)
    split_rh      = load_split_rh(period)

    if not fecs:
        print("\n❌ Aucun FEC chargé. Vérifiez data/fec/")
        return

    # ── 2. Calcul total MS par groupe RH ─────────────
    print("\n💼 Calcul masse salariale par groupe...\n")
    total_ms_by_group = {}

    for group, entities in RH_GROUPS.items():
        total = 0.0
        for entity in entities:
            if entity in fecs and entity in mappings:
                net    = compute_net_by_account(fecs[entity], period)
                mapped = apply_mapping(net, mappings[entity])
                total += mapped[mapped["mapping_pl_detail"] == "rh_to_allocate"]["montant_net"].sum()
        total_ms_by_group[group] = total
        print(f"  Groupe {group} : {total:,.0f} €")

    # ── 3. Transformations par entité ────────────────
    print("\n⚙️  Transformations...\n")
    transformed = {}

    for entity, fec in fecs.items():
        if entity not in mappings:
            print(f"  ⚠️  Mapping manquant pour {entity}, ignoré.")
            continue

        print(f"  -> {entity}")
        group = ENTITY_TO_RH_GROUP[entity]

        transformed[entity] = transform_entity(
            fec            = fec,
            mapping        = mappings[entity],
            split_ca       = split_ca,
            split_rh       = split_rh,
            entity         = entity,
            period         = period,
            total_ms_group = total_ms_by_group[group],
        )

    # ── 4. Construction des P&L ──────────────────────
    print("\n📊 Construction des P&L...\n")
    pl_dict = build_full_pl(transformed, pl_structures)

    # ── 5. Contrôles qualité ─────────────────────────
    print("\n🔍 Contrôles qualité...\n")
    controls_df = run_all_controls(fecs, mappings, split_rh, split_ca, pl_dict, period)
    errors   = (controls_df["statut"] == "❌ ERREUR").sum()
    warnings = (controls_df["statut"] == "⚠️  ATTENTION").sum()
    print(f"  {errors} erreur(s), {warnings} avertissement(s)")

    # ── 6. Export Excel ──────────────────────────────
    print("\n💾 Export Excel...\n")
    filepath = export_to_excel(pl_dict, period, pl_structures, controls_df)

    print(f"\n{'='*52}")
    print(f"  ✅ Terminé ! → {filepath}")
    print(f"{'='*52}\n")


# ─────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage : python main.py YYYYMM")
        print("Exemple : python main.py 202512")
        sys.exit(1)

    period = sys.argv[1]

    if len(period) != 6 or not period.isdigit():
        print("❌ Format invalide. Utilisez YYYYMM (ex: 202512)")
        sys.exit(1)

    run(period)