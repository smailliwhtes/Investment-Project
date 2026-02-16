from __future__ import annotations

import os
import queue
import subprocess
import sys
import threading
from dataclasses import dataclass
from pathlib import Path

import tkinter as tk
from tkinter import filedialog, messagebox, ttk

from market_app.ui.validation import (
    ValidationError,
    validate_config_path,
    validate_run_id,
    validate_runs_directory,
)


@dataclass(frozen=True)
class RunTargets:
    report_extensions: tuple[str, ...] = (".html", ".md", ".pdf")
    log_candidates: tuple[str, ...] = ("run.log", os.path.join("logs", "run.log"))


class MarketAppUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Market App - Minimal GUI")
        self.minsize(820, 560)

        self._status_var = tk.StringVar(value="Idle")
        self._log_queue: queue.Queue[str] = queue.Queue()
        self._process: subprocess.Popen[str] | None = None
        self._reader_thread: threading.Thread | None = None
        self._busy = False
        self._targets = RunTargets()

        defaults = self._default_paths()
        self._config_var = tk.StringVar(value=defaults["config"])
        self._runs_dir_var = tk.StringVar(value=defaults["runs_dir"])
        self._run_id_var = tk.StringVar(value="ui_run")
        self._offline_var = tk.BooleanVar(value=True)

        self._build_layout()
        self._poll_log_queue()
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _default_paths(self) -> dict[str, str]:
        repo_root = Path(__file__).resolve().parents[3]
        config_path = repo_root / "configs" / "acceptance.yaml"
        runs_dir = repo_root / "runs_acceptance"
        return {
            "config": str(config_path) if config_path.exists() else "",
            "runs_dir": str(runs_dir),
        }

    def _build_layout(self) -> None:
        padding = {"padx": 12, "pady": 6}

        form = ttk.Frame(self)
        form.pack(fill=tk.X, **padding)

        config_row = ttk.Frame(form)
        config_row.pack(fill=tk.X, pady=4)
        ttk.Label(config_row, text="Config").pack(side=tk.LEFT)
        ttk.Entry(config_row, textvariable=self._config_var, width=70).pack(
            side=tk.LEFT, padx=6, fill=tk.X, expand=True
        )
        ttk.Button(config_row, text="Browse", command=self._pick_config).pack(side=tk.LEFT)

        runs_row = ttk.Frame(form)
        runs_row.pack(fill=tk.X, pady=4)
        ttk.Label(runs_row, text="Runs dir").pack(side=tk.LEFT)
        ttk.Entry(runs_row, textvariable=self._runs_dir_var, width=70).pack(
            side=tk.LEFT, padx=6, fill=tk.X, expand=True
        )
        ttk.Button(runs_row, text="Browse", command=self._pick_runs_dir).pack(side=tk.LEFT)

        run_id_row = ttk.Frame(form)
        run_id_row.pack(fill=tk.X, pady=4)
        ttk.Label(run_id_row, text="Run ID").pack(side=tk.LEFT)
        ttk.Entry(run_id_row, textvariable=self._run_id_var, width=30).pack(
            side=tk.LEFT, padx=6
        )
        ttk.Checkbutton(run_id_row, text="Offline", variable=self._offline_var).pack(
            side=tk.LEFT, padx=12
        )

        actions = ttk.Frame(self)
        actions.pack(fill=tk.X, **padding)
        self._validate_button = ttk.Button(actions, text="Validate (doctor)", command=self._run_doctor)
        self._validate_button.pack(side=tk.LEFT, padx=4)
        self._run_button = ttk.Button(actions, text="Run pipeline", command=self._run_pipeline)
        self._run_button.pack(side=tk.LEFT, padx=4)
        ttk.Button(actions, text="Open latest report", command=self._open_latest_report).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(actions, text="Open latest log", command=self._open_latest_log).pack(
            side=tk.LEFT, padx=4
        )
        ttk.Button(actions, text="Open runs folder", command=self._open_runs_folder).pack(
            side=tk.LEFT, padx=4
        )

        status_row = ttk.Frame(self)
        status_row.pack(fill=tk.X, **padding)
        ttk.Label(status_row, text="Status:").pack(side=tk.LEFT)
        ttk.Label(status_row, textvariable=self._status_var).pack(side=tk.LEFT, padx=6)

        log_frame = ttk.Frame(self)
        log_frame.pack(fill=tk.BOTH, expand=True, **padding)
        ttk.Label(log_frame, text="Log").pack(anchor=tk.W)
        self._log_text = tk.Text(log_frame, height=20, wrap=tk.NONE)
        self._log_text.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scroll_y = ttk.Scrollbar(log_frame, orient=tk.VERTICAL, command=self._log_text.yview)
        scroll_y.pack(side=tk.RIGHT, fill=tk.Y)
        self._log_text.configure(yscrollcommand=scroll_y.set)

    def _pick_config(self) -> None:
        path = filedialog.askopenfilename(title="Select config", filetypes=[("YAML", "*.yaml")])
        if path:
            self._config_var.set(path)

    def _pick_runs_dir(self) -> None:
        path = filedialog.askdirectory(title="Select runs directory")
        if path:
            self._runs_dir_var.set(path)

    def _run_doctor(self) -> None:
        self._start_command(self._build_doctor_command(), "Doctor")

    def _run_pipeline(self) -> None:
        self._start_command(self._build_run_command(), "Run")

    def _build_doctor_command(self) -> list[str]:
        command = [sys.executable, "-m", "market_app.cli", "doctor", "--config", self._config_var.get()]
        if self._offline_var.get():
            command.append("--offline")
        return command

    def _build_run_command(self) -> list[str]:
        command = [
            sys.executable,
            "-m",
            "market_app.cli",
            "run",
            "--config",
            self._config_var.get(),
            "--runs-dir",
            self._runs_dir_var.get(),
            "--run-id",
            self._run_id_var.get(),
        ]
        if self._offline_var.get():
            command.append("--offline")
        return command

    def _start_command(self, command: list[str], label: str) -> None:
        if self._busy:
            messagebox.showinfo("Market App", "A command is already running.")
            return

        # Validate config path
        config_path_str = self._config_var.get().strip()
        try:
            config_path = validate_config_path(config_path_str)
        except ValidationError as exc:
            messagebox.showerror("Market App", str(exc))
            return

        # For the run command, also validate runs directory and run ID
        if label == "Run":
            runs_dir_str = self._runs_dir_var.get().strip()
            try:
                runs_dir = validate_runs_directory(runs_dir_str)
            except ValidationError as exc:
                messagebox.showerror("Market App", str(exc))
                return

            run_id_str = self._run_id_var.get().strip()
            try:
                run_id = validate_run_id(run_id_str)
            except ValidationError as exc:
                messagebox.showerror("Market App", str(exc))
                return

        self._set_busy(True)
        self._append_log(f"\n[{label}] Running: {' '.join(command)}\n")
        self._status_var.set(f"Running {label}")
        try:
            self._process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                text=True,
                bufsize=1,
            )
        except OSError as exc:
            self._append_log(f"[error] Failed to launch command: {exc}\n")
            self._status_var.set("Failed")
            self._set_busy(False)
            return

        self._reader_thread = threading.Thread(target=self._read_process_output, daemon=True)
        self._reader_thread.start()

    def _read_process_output(self) -> None:
        assert self._process is not None
        output = self._process.stdout
        if output is None:
            self._log_queue.put("[error] No process output available.\n")
            self._log_queue.put("__PROCESS_DONE__")
            return
        for line in output:
            self._log_queue.put(line)
        self._process.wait()
        self._log_queue.put("__PROCESS_DONE__")

    def _poll_log_queue(self) -> None:
        while True:
            try:
                line = self._log_queue.get_nowait()
            except queue.Empty:
                break
            if line == "__PROCESS_DONE__":
                self._finalize_process()
                continue
            self._append_log(line)
        self.after(100, self._poll_log_queue)

    def _append_log(self, message: str) -> None:
        self._log_text.insert(tk.END, message)
        self._log_text.see(tk.END)

    def _finalize_process(self) -> None:
        if not self._process:
            return
        exit_code = self._process.poll()
        status = "Success" if exit_code == 0 else f"Failed (exit {exit_code})"
        self._status_var.set(status)
        self._set_busy(False)
        self._process = None

    def _set_busy(self, busy: bool) -> None:
        self._busy = busy
        state = tk.DISABLED if busy else tk.NORMAL
        self._run_button.configure(state=state)
        self._validate_button.configure(state=state)

    def _open_latest_report(self) -> None:
        run_dir = self._latest_run_dir()
        if not run_dir:
            return
        report = self._find_report_file(run_dir)
        if not report:
            messagebox.showwarning("Market App", "No report found in latest run.")
            return
        self._open_path(report)

    def _open_latest_log(self) -> None:
        run_dir = self._latest_run_dir()
        if not run_dir:
            return
        log_path = self._find_log_file(run_dir)
        if not log_path:
            messagebox.showwarning("Market App", "No run.log found in latest run.")
            return
        self._open_path(log_path)

    def _open_runs_folder(self) -> None:
        runs_dir = Path(self._runs_dir_var.get())
        if not runs_dir.exists():
            messagebox.showwarning("Market App", "Runs directory does not exist.")
            return
        self._open_path(runs_dir)

    def _latest_run_dir(self) -> Path | None:
        runs_dir = Path(self._runs_dir_var.get())
        if not runs_dir.exists():
            messagebox.showwarning("Market App", "Runs directory does not exist.")
            return None
        run_dirs = [path for path in runs_dir.iterdir() if path.is_dir()]
        if not run_dirs:
            messagebox.showwarning("Market App", "No runs found in the runs directory.")
            return None
        return max(run_dirs, key=lambda path: path.stat().st_mtime)

    def _find_report_file(self, run_dir: Path) -> Path | None:
        for suffix in self._targets.report_extensions:
            candidate = run_dir / f"report{suffix}"
            if candidate.exists():
                return candidate
        return None

    def _find_log_file(self, run_dir: Path) -> Path | None:
        for relative in self._targets.log_candidates:
            candidate = run_dir / relative
            if candidate.exists():
                return candidate
        return None

    def _open_path(self, path: Path) -> None:
        try:
            if sys.platform.startswith("win"):
                os.startfile(path)  # type: ignore[attr-defined]
            elif sys.platform == "darwin":
                subprocess.run(["open", str(path)], check=False)
            else:
                subprocess.run(["xdg-open", str(path)], check=False)
        except OSError as exc:
            messagebox.showerror("Market App", f"Failed to open {path}: {exc}")

    def _on_close(self) -> None:
        if self._process and self._process.poll() is None:
            confirm = messagebox.askyesno(
                "Market App",
                "A command is still running. Do you want to stop it and exit?",
            )
            if not confirm:
                return
            self._process.terminate()
        self.destroy()


def run_ui() -> None:
    app = MarketAppUI()
    app.mainloop()


__all__ = ["MarketAppUI", "run_ui"]
