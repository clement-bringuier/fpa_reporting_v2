import sys
import pandas as pd
from src.loaders import (
    load_all_fec,
    load_all_mappings,
    load_pl_structure,
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

    # Chargement des structures P&L depuis le fichier mapping
    pl_structures = {
        "P&L PID":       load_pl_structure("Structure P&L PID"),
        "P&L Celsius":   load_pl_structure("Structure P&L CELSIUS"),
        "P&L Consolidé": load_pl_structure("Structure P&L Conso"),
    }
    print("✅ Structures P&L chargées")

    print("Split CA/COGS charge :", len(split_ca), "lignes")
    print("Colonnes split_ca :", split_ca.columns.tolist())
    print("Entites split_ca :", split_ca["entite"].unique().tolist())
    print("Split RH charge :", len(split_rh), "lignes")
    print("Colonnes split_rh :", split_rh.columns.tolist())
    print("Entites split_rh :", split_rh["entite"].unique().tolist())

    # DEBUG FEC PID
    fec_pid = fecs.get("PID")
    if fec_pid is not None:
        print("\n[DEBUG FEC PID] Shape:", fec_pid.shape)
        print("[DEBUG FEC PID] Colonnes:", fec_pid.columns.tolist())
        print("[DEBUG FEC PID] EcritureDate sample:", fec_pid["EcritureDate"].dropna().head(5).tolist())
        print("[DEBUG FEC PID] JournalCode unique:", fec_pid["JournalCode"].unique().tolist())
        p = pd.to_datetime("202512", format="%Y%m")
        fec_mois = fec_pid[
            (fec_pid["JournalCode"] != "AN") &
            (fec_pid["EcritureDate"].dt.year == p.year) &
            (fec_pid["EcritureDate"].dt.month == p.month)
        ]
        print("[DEBUG FEC PID] Lignes mois 202512 (hors AN):", len(fec_mois))
        comptes_7 = fec_mois[fec_mois["CompteNum"].str.startswith("7")]
        print("[DEBUG FEC PID] Lignes comptes 7x:", len(comptes_7))

    if not fecs:
        print("❌ Aucun FEC chargé. Vérifiez vos fichiers dans data/fec/")
        return

    # ── 2. Calcul total masse salariale par groupe ────
    from src.transformations import compute_net_by_account, apply_mapping

    ms_label = "Personnel costs to be allocated"
    groups_ms = {"PID+FR": ["FR", "PID"], "CELSIUS+VERTICAL": ["CELSIUS", "VERTICAL"]}
    total_ms_by_group = {}

    for group, entities in groups_ms.items():
        total = 0.0
        for ent in entities:
            if ent in fecs and ent in mappings:
                net = compute_net_by_account(fecs[ent], period)
                mapped = apply_mapping(net, mappings[ent])
                total += mapped[mapped["mapping_pl_detail"] == ms_label]["montant_net"].sum()
        total_ms_by_group[group] = total
        print(f"  Total MS groupe {group} : {total:.0f} €")

    # Mapping entité → total MS groupe
    entity_to_ms_group = {
        "FR":       total_ms_by_group["PID+FR"],
        "PID":      total_ms_by_group["PID+FR"],
        "CELSIUS":  total_ms_by_group["CELSIUS+VERTICAL"],
        "VERTICAL": total_ms_by_group["CELSIUS+VERTICAL"],
    }

    # ── 3. Transformations par entité ─────────────────
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
            period=period,
            total_ms_group=entity_to_ms_group.get(entity),
        )

    # ── 3. Construction des P&L ───────────────────────
    print("\n📊 Construction des P&L...\n")
    pl_dict = build_full_pl(transformed, pl_structures)

    # ── 4. Export Excel ───────────────────────────────
    print("\n💾 Export Excel...\n")
    filepath = export_to_excel(pl_dict, period, pl_structures)

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