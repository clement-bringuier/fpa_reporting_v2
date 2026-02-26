import sys
from src.loaders import (
    load_all_fec,
    load_all_mappings,
    load_split_ca_cogs,
    load_split_rh,
)
from src.transformations import transform_entity
from src.pl_builder import build_full_pl
from src.output import export_to_excel


def run(period: str):
    """
    Pipeline complet de génération du P&L.
    
    Args:
        period: format YYYYMM (ex: "202512")
    """
    print(f"\n{'='*50}")
    print(f"  GÉNÉRATION P&L — {period}")
    print(f"{'='*50}\n")

    # ── 1. Chargement des inputs ──────────────────────
    print("📂 Chargement des fichiers...\n")
    fecs        = load_all_fec(period)
    mappings    = load_all_mappings()
    split_ca    = load_split_ca_cogs(period)
    split_rh    = load_split_rh(period)

    if not fecs:
        print("❌ Aucun FEC chargé. Vérifiez vos fichiers dans data/fec/")
        return

    # ── 2. Transformations par entité ─────────────────
    print("\n⚙️  Transformations...\n")
    transformed = {}
    for entity, fec in fecs.items():
        if entity not in mappings:
            print(f"⚠️  Mapping manquant pour {entity}, entité ignorée.")
            continue
        print(f"  → {entity}")
        transformed[entity] = transform_entity(
            fec=fec,
            mapping=mappings[entity],
            split_ca_cogs=split_ca,
            split_rh=split_rh,
            entity=entity,
        )

    # ── 3. Construction des P&L ───────────────────────
    print("\n📊 Construction des P&L...\n")
    pl_dict = build_full_pl(transformed)

    # ── 4. Export Excel ───────────────────────────────
    print("\n💾 Export Excel...\n")
    filepath = export_to_excel(pl_dict, period)

    print(f"\n{'='*50}")
    print(f"  ✅ Terminé ! Fichier généré :")
    print(f"  {filepath}")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage : python main.py YYYYMM")
        print("Exemple : python main.py 202512")
        sys.exit(1)

    period = sys.argv[1]

    if len(period) != 6 or not period.isdigit():
        print("❌ Format de période invalide. Utilisez YYYYMM (ex: 202512)")
        sys.exit(1)

    run(period)