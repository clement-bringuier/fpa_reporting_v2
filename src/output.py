import pandas as pd
from openpyxl.styles import PatternFill, Font, Alignment, Border, Side
import os

# ─────────────────────────────────────────────
# STYLES
# ─────────────────────────────────────────────

def _fill(hex_color):
    return PatternFill("solid", fgColor=hex_color)

def _font(color="000000", bold=False, italic=False, size=10):
    return Font(color=color, bold=bold, italic=italic, size=size)

STYLE = {
    "header":     {"fill": _fill("2F3640"), "font": _font("FFFFFF", bold=True)},
    "margin_key": {"fill": _fill("1A6B9A"), "font": _font("FFFFFF", bold=True)},
    "total":      {"fill": _fill("636E72"), "font": _font("FFFFFF", bold=True)},
    "detail_0":   {"fill": _fill("FFFFFF"), "font": _font()},
    "detail_1":   {"fill": _fill("F5F6FA"), "font": _font()},
}

KEY_MARGINS   = {"Gross Margin", "Contribution Margin", "EBITDA", "EBIT"}
NUMBER_FORMAT = '#,##0;[Red]-#,##0'

_medium = Side(style="medium", color="000000")
BORDER_KEY = Border(top=_medium, bottom=_medium)


# ─────────────────────────────────────────────
# ÉCRITURE D'UN ONGLET P&L
# ─────────────────────────────────────────────

def write_pl_sheet(ws, pl_df: pd.DataFrame, title: str, period_label: str, structure: list):
    seen, label_type = set(), {}
    for _, label, t in structure:
        if label not in seen:
            label_type[label] = t
            seen.add(label)

    ws["A1"] = title
    ws["A1"].font = Font(bold=True, size=13, color="1A6B9A")
    ws["A2"] = period_label
    ws["A2"].font = Font(italic=True, size=10, color="636E72")

    for col, val in [("A4", ""), ("B4", period_label)]:
        cell = ws[col]
        cell.value = val
        cell.fill  = STYLE["header"]["fill"]
        cell.font  = STYLE["header"]["font"]
        cell.alignment = Alignment(horizontal="center")

    ws.column_dimensions["A"].width = 45
    ws.column_dimensions["B"].width = 18

    row, alt = 5, 0
    for label in pl_df.index:
        row_type = label_type.get(label, "detail")
        montant  = pl_df.loc[label, "Montant"]
        montant  = round(montant) if pd.notna(montant) else 0

        ca = ws.cell(row=row, column=1, value=label)
        cb = ws.cell(row=row, column=2, value=montant)
        cb.number_format = NUMBER_FORMAT
        cb.alignment     = Alignment(horizontal="right")

        if label in KEY_MARGINS:
            s = STYLE["margin_key"]
            ca.alignment = Alignment(horizontal="left")
            ca.border = cb.border = BORDER_KEY
        elif row_type in ("total", "margin"):
            s = STYLE["total"]
            ca.alignment = Alignment(horizontal="left")
        else:
            s   = STYLE[f"detail_{alt % 2}"]
            alt += 1
            ca.alignment = Alignment(horizontal="left", indent=2)

        ca.fill = cb.fill = s["fill"]
        ca.font = cb.font = s["font"]
        row += 1

    ws.freeze_panes = "A5"


# ─────────────────────────────────────────────
# ÉCRITURE DE L'ONGLET CONTRÔLES
# ─────────────────────────────────────────────

def write_controls_sheet(ws, controls_df: pd.DataFrame, period_label: str):
    STATUS_STYLE = {
        "✅ OK":         {"fill": _fill("DFF0D8"), "font": _font("2D6A2D")},
        "⚠️  ATTENTION": {"fill": _fill("FFF3CD"), "font": _font("856404")},
        "❌ ERREUR":     {"fill": _fill("F8D7DA"), "font": _font("721C24")},
    }

    ws["A1"] = "Contrôles qualité"
    ws["A1"].font = Font(bold=True, size=13, color="1A6B9A")
    ws["A2"] = period_label
    ws["A2"].font = Font(italic=True, size=10, color="636E72")

    headers    = ["Statut", "Détail", "Valeur"]
    col_widths = [22, 65, 60]
    for col_idx, (h, w) in enumerate(zip(headers, col_widths), start=1):
        cell = ws.cell(row=4, column=col_idx, value=h)
        cell.fill      = _fill("2F3640")
        cell.font      = _font("FFFFFF", bold=True)
        cell.alignment = Alignment(horizontal="center")
        ws.column_dimensions[cell.column_letter].width = w

    for row_idx, record in enumerate(controls_df.to_dict("records"), start=5):
        statut = record["statut"]
        style  = STATUS_STYLE.get(statut, STATUS_STYLE["⚠️  ATTENTION"])

        for col_idx, key in enumerate(["statut", "detail", "valeur"], start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=str(record[key]))
            cell.fill      = style["fill"]
            cell.font      = style["font"]
            cell.alignment = Alignment(horizontal="left", vertical="center", wrap_text=True)

        ws.row_dimensions[row_idx].height = 18

    ws.freeze_panes = "A5"


# ─────────────────────────────────────────────
# EXPORT EXCEL
# ─────────────────────────────────────────────

def export_to_excel(
    pl_dict: dict,
    period: str,
    pl_structures: dict,
    controls_df: pd.DataFrame,
    output_dir: str = "output"
) -> str:
    os.makedirs(output_dir, exist_ok=True)

    p            = pd.to_datetime(period, format="%Y%m")
    period_label = p.strftime("%B %Y").capitalize()
    filename     = os.path.join(output_dir, f"PL_{period}.xlsx")

    with pd.ExcelWriter(filename, engine="openpyxl") as writer:

        for sheet_name, pl_df in pl_dict.items():
            pl_df.to_excel(writer, sheet_name=sheet_name, index=True)
            ws = writer.sheets[sheet_name]
            for r in ws.iter_rows():
                for cell in r:
                    cell.value = None
            write_pl_sheet(ws, pl_df, sheet_name, period_label, pl_structures[sheet_name])

        controls_df.to_excel(writer, sheet_name="Contrôles", index=False)
        ws_ctrl = writer.sheets["Contrôles"]
        for r in ws_ctrl.iter_rows():
            for cell in r:
                cell.value = None
        write_controls_sheet(ws_ctrl, controls_df, period_label)

    print(f"  ✅ Fichier généré : {filename}")
    return filename