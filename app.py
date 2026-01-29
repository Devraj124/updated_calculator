from flask import Flask, render_template, request
import platform
import os
import time
import threading
import queue
import atexit

# Excel COM automation is Windows-only (pywin32).
try:
    import win32com.client  # type: ignore
    import pythoncom  # type: ignore
    import pywintypes  # type: ignore

    _PYWIN32_AVAILABLE = True
except Exception:  # pragma: no cover
    win32com = None  # type: ignore
    pythoncom = None  # type: ignore

    class _DummyComError(Exception):
        pass

    class _DummyPyWinTypes:
        com_error = _DummyComError

    pywintypes = _DummyPyWinTypes()  # type: ignore
    _PYWIN32_AVAILABLE = False

app = Flask(__name__)

EXCEL_FILE = os.path.abspath(os.path.join(os.path.dirname(__file__),
                          "Calculator_test.xlsb"))
SHEET_NAME = "Output"

EXCEL_TIMEOUT_S = float(os.environ.get("EXCEL_TIMEOUT_S", "90"))

DEFAULT_MAINTENANCE_MESSAGE = (
    "The calculator is under maintainence. We will notify you once its live"
)
# Create this file (empty is fine) to force maintenance mode:
# - enable:  create `maintenance.flag` next to app.py
# - disable: delete `maintenance.flag`
MAINTENANCE_FLAG_PATH = os.path.join(os.path.dirname(__file__), "maintenance.flag")


def _env_truthy(name: str) -> bool:
    v = os.environ.get(name)
    if v is None:
        return False
    return str(v).strip().lower() in ("1", "true", "yes", "y", "on")


def get_maintenance_message() -> str | None:
    # Manual switches (recommended for Excel updates)
    if _env_truthy("MAINTENANCE_MODE") or _env_truthy("CALCULATOR_MAINTENANCE"):
        return DEFAULT_MAINTENANCE_MESSAGE
    try:
        if os.path.exists(MAINTENANCE_FLAG_PATH):
            return DEFAULT_MAINTENANCE_MESSAGE
    except Exception:
        # If we can't check the flag, don't block the app.
        pass

    # Auto-switch: if the workbook isn't present, show maintenance notice.
    try:
        if not os.path.exists(EXCEL_FILE):
            return DEFAULT_MAINTENANCE_MESSAGE
    except Exception:
        pass

    # Render runs on Linux; Excel COM won't work there.
    if platform.system() != "Windows" or not _PYWIN32_AVAILABLE:
        return DEFAULT_MAINTENANCE_MESSAGE

    return None


def _should_show_maintenance_for_excel_error(e: Exception) -> bool:
    # If Excel is locked/recalculating/stuck or the file is being replaced,
    # show a friendly message instead of a raw traceback-style error.
    msg = str(e or "")
    if isinstance(e, (TimeoutError, FileNotFoundError)):
        return True
    if "Error opening/calculating Excel file" in msg:
        return True
    if "Excel operation timed out" in msg:
        return True
    if isinstance(e, pywintypes.com_error):
        return True
    return False


class _ExcelJob:
    __slots__ = ("fn", "args", "kwargs", "done", "result", "error")

    def __init__(self, fn, args, kwargs):
        self.fn = fn
        self.args = args
        self.kwargs = kwargs
        self.done = threading.Event()
        self.result = None
        self.error = None


class ExcelWorker:
    """
    Single-threaded Excel COM worker.

    Why: opening/closing Excel for every request is slow and can crash/hang due to COM "server busy".
    This worker keeps one hidden Excel instance + workbook open and serializes all requests.
    """

    def __init__(self, excel_file: str, sheet_name: str):
        self.excel_file = os.path.abspath(excel_file).replace("/", "\\")
        self.sheet_name = sheet_name

        self._q: "queue.Queue[_ExcelJob | None]" = queue.Queue()
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._start_lock = threading.Lock()
        self._started = False

        self.excel = None
        self.wb = None
        self.sheet = None

    def start(self):
        with self._start_lock:
            if self._started:
                return
            self._thread.start()
            self._started = True

    def stop(self, timeout_s: float = 5.0):
        if not self._started:
            return
        try:
            self._q.put_nowait(None)
        except Exception:
            pass
        try:
            self._thread.join(timeout=timeout_s)
        except Exception:
            pass

    def call(self, fn, *args, timeout_s: float = EXCEL_TIMEOUT_S, **kwargs):
        self.start()
        job = _ExcelJob(fn, args, kwargs)
        self._q.put(job)
        if not job.done.wait(timeout=timeout_s):
            raise TimeoutError(
                f"Excel operation timed out after {timeout_s:.0f}s. "
                "The workbook may be recalculating or Excel may be stuck."
            )
        if job.error is not None:
            raise job.error
        return job.result

    def _run(self):
        pythoncom.CoInitialize()
        try:
            while True:
                job = self._q.get()
                if job is None:
                    break
                try:
                    job.result = job.fn(*job.args, **job.kwargs)
                except Exception as e:
                    job.error = e
                finally:
                    job.done.set()
        finally:
            try:
                self._shutdown_excel()
            finally:
                try:
                    pythoncom.CoUninitialize()
                except Exception:
                    pass

    def _shutdown_excel(self):
        # Close workbook and quit Excel (worker thread only)
        try:
            if self.wb:
                _com_retry(lambda: self.wb.Close(SaveChanges=False), retries=5, delay_s=0.1)
        except Exception:
            pass
        try:
            if self.excel:
                _com_retry(lambda: self.excel.Quit(), retries=5, delay_s=0.1)
        except Exception:
            pass
        self.sheet = None
        self.wb = None
        self.excel = None

    def _restart_excel(self):
        self._shutdown_excel()
        self._ensure_open()

    def _ensure_open(self):
        if self.excel is not None and self.wb is not None and self.sheet is not None:
            return

        if not os.path.exists(self.excel_file):
            raise FileNotFoundError(f"Excel file not found: {self.excel_file}")

        self.excel = _com_retry(lambda: win32com.client.DispatchEx("Excel.Application"))
        self.excel.Visible = False
        self.excel.DisplayAlerts = False
        self.excel.EnableEvents = False
        self.excel.ScreenUpdating = False
        try:
            # xlCalculationAutomatic = -4105
            self.excel.Calculation = -4105
        except Exception:
            pass
        try:
            # Prevent link-update prompts
            self.excel.AskToUpdateLinks = False
        except Exception:
            pass
        try:
            # Disable macros on open (prevents hidden security prompts/hangs)
            # msoAutomationSecurityForceDisable = 3
            self.excel.AutomationSecurity = 3
        except Exception:
            pass

        def _try_open_workbook(corrupt_load: int = 0):
            # IgnoreReadOnlyRecommended avoids prompts that can hang automation.
            return self.excel.Workbooks.Open(
                self.excel_file,
                UpdateLinks=0,  # 0 = xlUpdateLinksNever
                ReadOnly=True,  # We don't save; avoids file-lock collisions
                IgnoreReadOnlyRecommended=True,
                AddToMru=False,
                Notify=False,
                CorruptLoad=corrupt_load,  # 0=normal, 1=repair
            )

        # Some files open in Protected View (downloaded/blocked). In that case Workbooks.Open can fail.
        # Fall back to ProtectedViewWindows.Open then "Edit" it.
        try:
            self.wb = _com_retry(lambda: _try_open_workbook(0), retries=30, delay_s=0.2)
        except pywintypes.com_error:
            try:
                self.wb = _com_retry(lambda: _try_open_workbook(1), retries=30, delay_s=0.2)
            except pywintypes.com_error:
                pvw = None
                try:
                    pvw = _com_retry(
                        lambda: self.excel.ProtectedViewWindows.Open(self.excel_file),
                        retries=10,
                        delay_s=0.25,
                    )
                    _com_retry(lambda: pvw.Edit(), retries=10, delay_s=0.25)
                    self.wb = _com_retry(lambda: pvw.Workbook, retries=10, delay_s=0.25)
                finally:
                    try:
                        if pvw is not None:
                            _com_retry(lambda: pvw.Close(), retries=3, delay_s=0.1)
                    except Exception:
                        pass

        self.sheet = _com_retry(lambda: self.wb.Sheets(self.sheet_name))

    def _wait_for_calc_done(self, timeout_s: float = 30.0):
        # Excel.CalculationState: 0=xlDone, 1=xlCalculating, 2=xlPending
        start = time.time()
        while True:
            try:
                state = int(self.excel.CalculationState)
            except Exception:
                return
            if state == 0:
                return
            if time.time() - start > timeout_s:
                return
            time.sleep(0.05)


excel_worker = ExcelWorker(EXCEL_FILE, SHEET_NAME)


@atexit.register
def _shutdown_worker():
    try:
        excel_worker.stop(timeout_s=3.0)
    except Exception:
        pass


def _is_retryable_excel_com_error(e: Exception) -> bool:
    # Common Excel "busy" errors
    if not isinstance(e, pywintypes.com_error):
        return False
    try:
        return e.hresult in (
            -2147418111,  # Call was rejected by callee
            -2147417846,  # Server busy
            -2147023174,  # The RPC server is unavailable
            -2147352567,  # Exception occurred (often wraps transient Workbooks.Open failures)
        )
    except Exception:
        return False


def _com_retry(fn, retries: int = 20, delay_s: float = 0.15):
    last = None
    for attempt in range(retries):
        try:
            return fn()
        except pywintypes.com_error as e:
            last = e
            if _is_retryable_excel_com_error(e) and attempt < retries - 1:
                time.sleep(delay_s)
                continue
            raise
    raise last  # pragma: no cover


def read_excel(
    emp_id,
    desired_earning,
    non_par,
    par,
    term,
    *,
    product_rows=None,
    rows_to_display=1,
    clear_new_rows_from=None,
):
    def _calculate_once():
        excel_worker._ensure_open()
        sheet = excel_worker.sheet

        # IMPORTANT: workbook is kept open between requests, so clear prior inputs.
        # Only clear INPUT columns (do NOT clear P/W/X which may contain formulas).
        try:
            _com_retry(lambda: sheet.Range("O5:O14").ClearContents())
            _com_retry(lambda: sheet.Range("Q5:V14").ClearContents())
        except Exception:
            pass

        # ---------- INPUT ----------
        sheet.Range("D5").Value = emp_id
        sheet.Range("D7").Value = desired_earning

        # Convert percentage inputs to decimal format for Excel (30 -> 0.30 for 30%)
        non_par_decimal = float(non_par) / 100 if non_par else 0
        par_decimal = float(par) / 100 if par else 0
        term_decimal = float(term) / 100 if term else 0

        sheet.Range("D19").Value = non_par_decimal
        sheet.Range("D20").Value = par_decimal
        sheet.Range("D21").Value = term_decimal

        sheet.Range("D19").NumberFormat = "0%"
        sheet.Range("D20").NumberFormat = "0%"
        sheet.Range("D21").NumberFormat = "0%"

        # ---------- INPUT (Individual Product rows: write O/Q/R/S/T/U/V starting row 5) ----------
        rows = product_rows or []
        for idx, row in enumerate(rows):
            excel_row = 5 + idx
            sheet.Range(f"O{excel_row}").Value = str(row.get("product", "")).strip()
            sheet.Range(f"Q{excel_row}").Value = str(row.get("rop_variant_yn", "")).strip()
            sheet.Range(f"R{excel_row}").Value = str(row.get("cashback_yn", "")).strip()
            sheet.Range(f"S{excel_row}").Value = int(row.get("ppt"))
            sheet.Range(f"T{excel_row}").Value = int(row.get("pt"))
            sheet.Range(f"U{excel_row}").Value = float(row.get("epi"))
            sheet.Range(f"V{excel_row}").Value = float(row.get("rider_prem"))

        # If we're adding a new row in UI, clear the newly-added Excel row(s) so stale values don't show.
        if clear_new_rows_from is not None:
            try:
                start_clear = int(clear_new_rows_from)
                end_clear = 4 + int(rows_to_display)  # last Excel row used
                for excel_row in range(start_clear, end_clear + 1):
                    sheet.Range(f"O{excel_row}").Value = ""
                    sheet.Range(f"Q{excel_row}").Value = ""
                    sheet.Range(f"R{excel_row}").Value = ""
                    sheet.Range(f"S{excel_row}").Value = ""
                    sheet.Range(f"T{excel_row}").Value = ""
                    sheet.Range(f"U{excel_row}").Value = ""
                    sheet.Range(f"V{excel_row}").Value = ""
            except Exception:
                pass

        # Force calculation (prefer smaller-scope calc; fall back to full)
        try:
            _com_retry(lambda: sheet.Calculate())
            excel_worker._wait_for_calc_done(timeout_s=30.0)
        except Exception:
            try:
                _com_retry(lambda: excel_worker.excel.Calculate())
                excel_worker._wait_for_calc_done(timeout_s=30.0)
            except Exception:
                _com_retry(lambda: excel_worker.excel.CalculateFull())
                excel_worker._wait_for_calc_done(timeout_s=60.0)

        # ---------- OUTPUT (Single values) ----------
        try:
            h7_value = sheet.Range("H7").Value
            if h7_value is None:
                h7_value = 0
            else:
                h7_value = float(h7_value)
        except Exception:
            h7_value = 0

        result = {
            "name": sheet.Range("I5").Text,
            "channel": sheet.Range("J6").Text,
            "total_target": sheet.Range("D9").Text,
            "total_biz": sheet.Range("I9").Text,
            "achievement": sheet.Range("J9").Text,
            "deficit": sheet.Range("I12").Text,
            "incentive": sheet.Range("I14").Text,
            "remaining_amount": sheet.Range("I15").Text,
            "capping": h7_value,  # H7 value for validation
        }

        # ---------- OUTPUT (Product mix table C11:F15) ----------
        product_mix = []
        range_obj = sheet.Range("C11:F15")
        for row_idx in range(2, range_obj.Rows.Count + 1):  # Start from 2 to skip header row
            row = []
            for col_idx in range(1, range_obj.Columns.Count + 1):
                cell = range_obj.Cells(row_idx, col_idx)
                if col_idx == 1:
                    row.append(cell.Text)
                elif col_idx == 2:
                    try:
                        val = cell.Value
                        if val is not None:
                            row.append(int(float(val)))
                        else:
                            row.append("")
                    except Exception:
                        row.append(cell.Text)
                else:
                    row.append(cell.Text)
            product_mix.append(row)
        result["product_mix"] = product_mix

        # ---------- OUTPUT (Hit 100% line) ----------
        result["hit_100"] = sheet.Range("C17").Text

        # ---------- OUTPUT (Below Hit 100% table) ----------
        below_table = []
        for row_num in (19, 20, 21):
            below_table.append(
                [
                    sheet.Range(f"D{row_num}").Text,
                    sheet.Range(f"E{row_num}").Text,
                    sheet.Range(f"J{row_num}").Text,
                ]
            )
        result["below_table"] = below_table

        # ---------- OUTPUT (Performance on other Parameters) ----------
        result["performance_params"] = [
            {"label": sheet.Range("C27").Text, "value": sheet.Range("D27").Text},
            {"label": sheet.Range("C28").Text, "value": sheet.Range("D28").Text},
        ]

        # ---------- OUTPUT (Individual Product Mix - headers N4:X4, rows starting 5) ----------
        individual_headers = []
        for col in range(14, 25):  # N=14 ... X=24
            try:
                cell = sheet.Cells(4, col)
                individual_headers.append(cell.Text if cell.Text else "")
            except Exception:
                individual_headers.append("")
        result["individual_product_headers"] = individual_headers

        try:
            rows_to_display_int = max(1, min(10, int(rows_to_display)))
        except Exception:
            rows_to_display_int = 1
        result["individual_product_row_count"] = rows_to_display_int

        def cell_display(cell):
            try:
                v = cell.Value
                if v is None:
                    return cell.Text or ""
                if isinstance(v, (int, float)):
                    if isinstance(v, float) and v.is_integer():
                        return str(int(v))
                    if isinstance(v, int):
                        return str(v)
                    return str(v)
                return str(v).strip() if v else (cell.Text or "")
            except Exception:
                return cell.Text or ""

        rows_out = []
        for idx in range(rows_to_display_int):
            excel_row = 5 + idx
            rows_out.append(
                {
                    "product": sheet.Range(f"O{excel_row}").Text or "",
                    "variant": sheet.Range(f"P{excel_row}").Text or "",
                    "rop_variant_yn": sheet.Range(f"Q{excel_row}").Text or "",
                    "cashback_yn": sheet.Range(f"R{excel_row}").Text or "",
                    "ppt": cell_display(sheet.Range(f"S{excel_row}")),
                    "pt": cell_display(sheet.Range(f"T{excel_row}")),
                    "epi": cell_display(sheet.Range(f"U{excel_row}")),
                    "rider_prem": cell_display(sheet.Range(f"V{excel_row}")),
                    "incentive_per_product": cell_display(sheet.Range(f"W{excel_row}")),
                    "total_incentive": cell_display(sheet.Range(f"X{excel_row}")),
                }
            )
        result["individual_product_rows"] = rows_out

        return result

    def _calculate_with_restart():
        try:
            return _calculate_once()
        except pywintypes.com_error as e:
            if _is_retryable_excel_com_error(e):
                try:
                    excel_worker._restart_excel()
                except Exception:
                    pass
                return _calculate_once()
            raise
        except Exception:
            # If something broke Excel state, restart once and retry
            try:
                excel_worker._restart_excel()
                return _calculate_once()
            except Exception:
                raise

    try:
        return excel_worker.call(_calculate_with_restart, timeout_s=EXCEL_TIMEOUT_S)
    except Exception as e:
        error_msg = f"Error opening/calculating Excel file: {str(e)}\n\nFile path: {EXCEL_FILE}\n\n"
        if _is_retryable_excel_com_error(e):
            error_msg += "Excel appears busy. Please close any Excel dialogs/popups and try again.\n"
        raise Exception(error_msg)


@app.route("/", methods=["GET", "POST"])
def index():
    result = None

    error_message = None
    capping_value = None
    maintenance_message = get_maintenance_message()

    # Hard maintenance gate (Excel being updated / app temporarily unavailable)
    if maintenance_message:
        return render_template(
            "index.html",
            result=None,
            error_message=None,
            maintenance_message=maintenance_message,
            capping_value=None,
        )
    
    if request.method == "POST":
        emp_id = request.form.get("emp_id")
        desired_earning = request.form.get("desired_earning")
        non_par = request.form.get("non_par")
        par = request.form.get("par")
        term = request.form.get("term")

        # Individual product table state
        action = request.form.get("action")  # "add_row" when clicking Add more products
        try:
            row_count = int(request.form.get("row_count") or 1)
        except Exception:
            row_count = 1
        row_count = max(1, min(10, row_count))

        product_rows = []
        has_product_table = request.form.get("row_count") is not None or request.form.get("product_1") is not None

        # Parse rows from form (1..row_count)
        if has_product_table:
            for i in range(1, row_count + 1):
                product = (request.form.get(f"product_{i}") or "").strip()
                rop_variant_yn = (request.form.get(f"rop_variant_yn_{i}") or "").strip()
                cashback_yn = (request.form.get(f"cashback_yn_{i}") or "").strip()
                ppt_raw = (request.form.get(f"ppt_{i}") or "").strip()
                pt_raw = (request.form.get(f"pt_{i}") or "").strip()
                epi_raw = (request.form.get(f"epi_{i}") or "").strip()
                rider_raw = (request.form.get(f"rider_prem_{i}") or "").strip()

                # Server-side validation (keep strict even though button is disabled client-side)
                if not product or not rop_variant_yn or not cashback_yn or not ppt_raw or not pt_raw or not epi_raw or not rider_raw:
                    error_message = "Please fill all Individual Product Mix fields before adding more products."
                    break
                if rop_variant_yn not in ("Y", "N") or cashback_yn not in ("Y", "N"):
                    error_message = "ROP Variant and Cashback must be Y or N."
                    break
                try:
                    ppt_val = int(ppt_raw)
                    pt_val = int(pt_raw)
                except Exception:
                    error_message = "PPT and PT must be whole numbers."
                    break
                if ppt_val > 30:
                    error_message = "Please enter the correct PPT"
                    break
                if ppt_val <= 0 or pt_val <= 0:
                    error_message = "Please fill all Individual Product Mix fields before adding more products."
                    break
                if str(ppt_raw).strip().find(".") != -1 or str(pt_raw).strip().find(".") != -1:
                    error_message = "PPT and PT must be whole numbers (no decimals)."
                    break
                if pt_val < ppt_val:
                    error_message = "PT cannot be less than PPT."
                    break
                try:
                    epi_val = float(epi_raw)
                    rider_val = float(rider_raw)
                except Exception:
                    error_message = "EPI and Rider Premium must be numbers."
                    break
                if epi_val <= 0 or rider_val <= 0:
                    error_message = "Please fill all Individual Product Mix fields before adding more products."
                    break

                product_rows.append(
                    {
                        "product": product,
                        "rop_variant_yn": rop_variant_yn,
                        "cashback_yn": cashback_yn,
                        "ppt": ppt_val,
                        "pt": pt_val,
                        "epi": epi_val,
                        "rider_prem": rider_val,
                    }
                )

        # Add-row action increases visible rows (max 10)
        rows_to_display = row_count
        clear_new_rows_from = None
        if error_message is None and action == "add_row" and row_count < 10:
            rows_to_display = row_count + 1
            clear_new_rows_from = 5 + row_count  # Excel row of the newly added UI row

        try:
            result = read_excel(
                emp_id,
                desired_earning,
                non_par,
                par,
                term,
                product_rows=product_rows,
                rows_to_display=rows_to_display,
                clear_new_rows_from=clear_new_rows_from,
            )

            # Always pass capping back to UI for client-side validation on reload.
            try:
                capping_value = float(result.get("capping") or 0)
            except Exception:
                capping_value = None

            # If desired earning is above capping on the first attempt, do NOT show results.
            # Instead, ask user to adjust desired earning and re-submit.
            try:
                desired_val = float(desired_earning) if desired_earning not in (None, "") else None
            except Exception:
                desired_val = None
            if (
                desired_val is not None
                and capping_value is not None
                and capping_value > 0
                and desired_val > capping_value
            ):
                # Keep the page "clean": no results shown, and JS will show inline message +
                # disable Calculate until corrected.
                result = None
                error_message = None
        except Exception as e:
            # If Excel is unavailable (common during workbook updates), show maintenance message.
            if _should_show_maintenance_for_excel_error(e):
                try:
                    app.logger.exception("Excel unavailable; showing maintenance message")
                except Exception:
                    pass
                maintenance_message = DEFAULT_MAINTENANCE_MESSAGE
                error_message = None
            else:
                error_message = str(e)
            result = None

    return render_template(
        "index.html",
        result=result,
        error_message=error_message,
        maintenance_message=maintenance_message,
        capping_value=capping_value,
    )


if __name__ == "__main__":
    # Avoid Flask reloader/threading causing concurrent Excel COM calls
    app.run(debug=True, use_reloader=False, threaded=False)
