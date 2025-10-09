from __future__ import annotations
from typing import Optional, Dict, Any, List, Tuple, DefaultDict
from pathlib import Path
import os
import shutil
from collections import defaultdict

# Internal utilities
from .utilis import load_catalog_core, download_fim_assets, _to_date, _to_hour_or_none, _record_day, _record_hour_or_none, format_records_for_print,find_fims             

from ..datadownload import DownloadHUC8, setup_directories
from ..streamflowdata.nwmretrospective import getNWMretrospectivedata
from ..runFIM import runOWPHANDFIM

class FIMService:
    """
    - query(huc8, date_input=None, file_name=None) -> strict matches + pretty listing
    - process(..., ensure_owp, generate_owp_if_missing, base_dir=None, file_name=None)
      Creates folders {CWD}/FIM_evaluation/FIM_inputs/HUC{huc}_flood{YYYYMMDD[HHMMSS]}
      Downloads ONLY the matched record(s) (and their gpkg) into that folder.
    """
    _, _, out_root = setup_directories()
    default_root = out_root
    owp_root = Path(os.getenv("OWP_OUT_ROOT", default_root))

    def availability(self, HUCID: str) -> str:
        from .utilis import availability as _avail
        return _avail(HUCID)

    def query(
        self,
        HUCID: str,
        date_input: Optional[str] = None,
        file_name: Optional[str] = None,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> Dict[str, Any]:
        catalog = load_catalog_core()
        records = catalog.get("records", [])
        huc8 = str(HUCID).strip()

        # strict set
        strict_matches = find_fims(
            records, huc8=huc8, date_input=date_input, file_name=file_name,
            relaxed_for_print=False
        )

        # relaxed set
        relaxed_matches = find_fims(
            records, huc8=huc8, date_input=date_input, file_name=file_name,
            start_date=start_date, end_date=end_date, relaxed_for_print=True
        )

        status = "ok" if strict_matches else ("info" if (date_input is None and file_name is None and not start_date and not end_date) else "not_found")

        base_msg = f"Found {len(strict_matches)} record(s) for HUC {huc8}"
        if date_input:
            base_msg += f" and '{date_input}'"
        if file_name:
            base_msg += f" and file '{file_name}'"
        if start_date or end_date:
            base_msg += f" in range [{start_date or '-∞'} , {end_date or '∞'}]"
        if not strict_matches:
            base_msg = "No match for HUC " + huc8 \
                    + (f" and '{date_input}'" if date_input else "") \
                    + (f" and file '{file_name}'" if file_name else "") \
                    + (f" in range [{start_date or '-∞'} , {end_date or '∞'}]" if (start_date or end_date) else "")

        printable = format_records_for_print(relaxed_matches)
        return {
            "status": status,
            "message": base_msg + "\n" + self.availability(huc8),
            "matches": strict_matches,
            "printable": printable,
        }
    # Trigger the process to download the benchmark FIM data and ensure/generate the OWP HAND FIM
    def process(
        self,
        huc8: str,
        date_input: Optional[str] = None,
        ensure_owp: bool = True,
        generate_owp_if_missing: bool = True,
        out_dir: Optional[str] = None,
        file_name: Optional[str] = None,
    ) -> Dict[str, Any]:
        catalog = load_catalog_core()
        records = catalog.get("records", [])
        strict_matches = find_fims(
            records,
            huc8=str(huc8).strip(),
            date_input=date_input,
            file_name=file_name,
            start_date=None,
            end_date=None,
            relaxed_for_print=False,
        )

        # If strict match missing but filename given → fallback to filename-based lookup
        if not strict_matches:
            if file_name:
                fname = file_name.strip()
                cand_same_huc = [
                    r for r in records
                    if str(r.get("file_name", "")).strip() == fname
                    and str(r.get("huc8", "")).strip() == str(huc8).strip()
                ]
                cand_any_huc = [r for r in records if str(r.get("file_name", "")).strip() == fname]
                rec = cand_same_huc[0] if cand_same_huc else (cand_any_huc[0] if cand_any_huc else None)

                if rec is None:
                    msg = (
                        f"No strict benchmark match for HUC {huc8}"
                        + (f" and '{date_input}'" if date_input else "")
                        + f", and file '{file_name}' not found in catalog."
                    )
                    return {"status": "not_found", "message": msg, "folders": [], "matches": []}

                root = Path(out_dir or os.getcwd())
                inputs_root = root / "FIM_evaluation" / "FIM_inputs"
                inputs_root.mkdir(parents=True, exist_ok=True)

                # IMPORTANT: if user provided date_input, force folder label from user date
                if date_input:
                    label = self._date_label_from_user(date_input)
                else:
                    try:
                        label = self._date_label_for_record(rec)
                    except Exception:
                        label = "unknown"

                folder = inputs_root / f"HUC{huc8}_flood{label}"
                folder.mkdir(parents=True, exist_ok=True)

                dl = download_fim_assets(rec, str(folder))

                owp_path = None
                if ensure_owp and date_input:
                    owp_path = self._ensure_owp_to(
                        huc8, date_input, str(folder), generate_if_missing=generate_owp_if_missing
                    )

                msg = (
                    f"Used user-specified file '{file_name}' as benchmark reference. "
                    f"Downloaded into '{folder}'."
                )
                if ensure_owp and date_input:
                    msg += (
                        " OWP HAND FIM ensured (copied/generated)."
                        if owp_path
                        else " OWP HAND FIM not found and not generated."
                    )

                return {
                    "status": "assumed",
                    "message": msg,
                    "folders": [{
                        "label": label,
                        "folder": str(folder),
                        "records": [rec],
                        "downloads": [{"record": rec, "downloads": dl}],
                        "owp_path": owp_path,
                    }],
                    "matches": [rec],
                }

            # No file_name provided → truly no match
            msg = (f"No strict benchmark match for HUC {huc8}"
                + (f" and '{date_input}'" if date_input else ""))
            return {"status": "not_found", "message": msg, "folders": [], "matches": []}

        # Normal strict match path
        root = Path(out_dir or os.getcwd())
        inputs_root = root / "FIM_evaluation" / "FIM_inputs"
        inputs_root.mkdir(parents=True, exist_ok=True)

        total_downloaded = 0
        ensured = False
        ensured_path: Optional[str] = None
        folders_out: List[Dict[str, Any]] = []

        if date_input:
            # SINGLE folder named from user-provided date; put everything here
            user_label = self._date_label_from_user(date_input)
            folder = inputs_root / f"HUC{huc8}_flood{user_label}"
            folder.mkdir(parents=True, exist_ok=True)

            dl_records: List[Dict[str, Any]] = []
            for rec in strict_matches:
                dl = download_fim_assets(rec, str(folder))
                dl_records.append({"record": rec, "downloads": dl})
                if dl.get("tif") or dl.get("gpkg_files"):
                    total_downloaded += 1

            owp_path: Optional[str] = None
            if ensure_owp:
                owp_path = self._ensure_owp_to(
                    huc8, date_input, str(folder), generate_if_missing=generate_owp_if_missing
                )
                ensured = bool(owp_path)
                ensured_path = owp_path

            folders_out.append({
                "label": user_label,
                "folder": str(folder),
                "records": strict_matches,
                "downloads": dl_records,
                "owp_path": owp_path,
            })

        else:
            # No user date → keep legacy per-record grouping
            groups: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
            for r in strict_matches:
                label = self._date_label_for_record(r)
                groups[label].append(r)

            for label, recs in sorted(groups.items()):
                folder = inputs_root / f"HUC{huc8}_flood{label}"
                folder.mkdir(parents=True, exist_ok=True)

                dl_records: List[Dict[str, Any]] = []
                for rec in recs:
                    dl = download_fim_assets(rec, str(folder))
                    dl_records.append({"record": rec, "downloads": dl})
                    if dl.get("tif") or dl.get("gpkg_files"):
                        total_downloaded += 1

                folders_out.append({
                    "label": label,
                    "folder": str(folder),
                    "records": recs,
                    "downloads": dl_records,
                    "owp_path": None,  # no exact user date to target here
                })

        msg_bits = [f"Downloaded {total_downloaded} benchmark item(s) into '{inputs_root}'."]
        if ensure_owp and date_input:
            if ensured:
                msg_bits.append(f"OWP HAND FIM ensured for '{date_input}' (copied/generated).")
            else:
                msg_bits.append(f"OWP HAND FIM not found for '{date_input}' and was not generated.")

        return {
            "status": "ok",
            "message": " ".join(msg_bits),
            "folders": folders_out,
            "matches": strict_matches,
        }


    # Internals
    @staticmethod
    def _date_label_for_record(rec: Dict[str, Any]) -> str:
        day = _record_day(rec)
        hh = _record_hour_or_none(rec)
        if not day:
            raw = str(rec.get("date_raw", ""))[:8]
            return raw if raw else "unknown"
        if hh is None:
            return f"{day:%Y%m%d}"
        return f"{day:%Y%m%d}{hh:02d}0000"

    @staticmethod
    def _date_label_from_user(user_dt: str) -> str:
        day = _to_date(user_dt)
        hh = _to_hour_or_none(user_dt)
        return f"{day:%Y%m%d}" if hh is None else f"{day:%Y%m%d}{hh:02d}0000"

    def _ensure_owp_to(self, huc8: str, user_dt: str, dest_dir: str, generate_if_missing: bool) -> Optional[str]:
        existing = self._find_existing_owp_tif(huc8, user_dt)
        if existing:
            return self._copy_to_dest(existing, dest_dir)
        if generate_if_missing:
            produced = self._generate_owp(huc8, user_dt)
            if produced and produced.exists():
                return self._copy_to_dest(produced, dest_dir)
        return None

    # OWP helpers
    def _ymd_timestr_from_user(self, user_dt: str) -> Tuple[str, Optional[str]]:
        day = _to_date(user_dt)
        hh = _to_hour_or_none(user_dt)
        ymd = day.strftime("%Y%m%d")
        return (ymd, None) if hh is None else (ymd, f"{hh:02d}0000")

    def _expected_owp_path(self, huc8: str, ymd: str, timestr: Optional[str]) -> Path:
        name = f"NWM_{ymd}_{huc8}_inundation.tif" if timestr is None else f"NWM_{ymd}{timestr}_{huc8}_inundation.tif"
        return self.owp_root / f"flood_{huc8}" / f"{huc8}_inundation" / name

    def _find_existing_owp_tif(self, huc8: str, user_dt: str) -> Optional[Path]:
        ymd, timestr = self._ymd_timestr_from_user(user_dt)
        cand = self._expected_owp_path(huc8, ymd, timestr)
        if cand.exists():
            return cand
        if timestr is None:
            return None
        return None

    @staticmethod
    def _copy_to_dest(src: Path, dest_dir: str) -> str:
        dst_dir = Path(dest_dir)
        dst_dir.mkdir(parents=True, exist_ok=True)
        dst = dst_dir / src.name
        shutil.copy2(src, dst)
        return str(dst)

    def _generate_owp(self, huc8: str, user_dt: str) -> Optional[Path]:
        print(f"[owp] Generating for HUC {huc8} and '{user_dt}'...")
        # Inputs
        DownloadHUC8(huc8, version="4.8")
        # NWM retrospective for the exact stamp
        day = _to_date(user_dt); hh = _to_hour_or_none(user_dt)
        stamp = f"{day:%Y-%m-%d}" if hh is None else f"{day:%Y-%m-%d} {hh:02d}:00:00"
        getNWMretrospectivedata(huc_event_dict={str(huc8): [stamp]})
        # Run model
        runOWPHANDFIM(huc8)
        # Expected output
        ymd, timestr = self._ymd_timestr_from_user(user_dt)
        expected = self._expected_owp_path(huc8, ymd, timestr)
        return expected if expected.exists() else None

# FIM lookup convenience function
def fim_lookup(
    HUCID: str,
    date_input: Optional[str] = None,
    file_name: Optional[str] = None,
    run_handfim: bool = False,
    out_dir: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
) -> str:
    """
    - run_handfim=False (default): show a formatted benchmark list.
    - run_handfim=True: run the OWP HAND process (copy/generate), DO NOT print the benchmark summary;
      just return the operational message from the process step.
    """
    svc = FIMService()

    # List-only mode
    if not run_handfim:
        q = svc.query(
            HUCID=HUCID,
            date_input=date_input,
            file_name=file_name,
            start_date=start_date,
            end_date=end_date,
        )
        txt = q.get("printable") or ""
        if not txt.strip():
            return ("No benchmark FIMs were matched with the information you provided.\n"
                    f"(HUC={HUCID}"
                    f"{', date='+date_input if date_input else ''}"
                    f"{', file_name='+file_name if file_name else ''}"
                    f"{', range=['+str(start_date)+' , '+str(end_date)+']' if (start_date or end_date) else ''})")

        header = "Following are the available benchmark data"
        filt = []
        if HUCID: filt.append(f"HUC {HUCID}")
        if date_input: filt.append(f"date '{date_input}'")
        if start_date or end_date: filt.append(f"range [{start_date or '-∞'} , {end_date or '∞'}]")
        if file_name: filt.append(f"file '{file_name}'")
        prefix = header + (" for " + ", ".join(filt) + ":\n" if filt else ":\n")
        return prefix + txt

    # Run/ensure OWP mode
    rep = svc.process(
        huc8=HUCID,
        date_input=date_input,
        ensure_owp=True,
        generate_owp_if_missing=True,
        out_dir=out_dir,
        file_name=file_name,
    )
    return rep.get("message", "")


