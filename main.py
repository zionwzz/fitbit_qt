from pathlib import Path
import traceback

from PySide6.QtCore import QObject, QThread, Signal, Slot, QDate, QAbstractTableModel, Qt, QModelIndex
from PySide6.QtWidgets import (
    QApplication,
    QCheckBox,
    QDateEdit,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QTabWidget,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from core.converter import convert_takeout_zip_bytes, sanitize_filename_part


ACTIVITY_COLUMNS = [
    "Date",
    "Calories Burned",
    "Steps",
    "Distance",
    "Floors",
    "Minutes Sedentary",
    "Minutes Lightly Active",
    "Minutes Fairly Active",
    "Minutes Very Active",
    "Activity Calories",
]

SLEEP_COLUMNS = [
    "Start Time",
    "End Time",
    "Minutes Asleep",
    "Minutes Awake",
    "Number of Awakenings",
    "Time in Bed",
    "Minutes REM Sleep",
    "Minutes Light Sleep",
    "Minutes Deep Sleep",
]


class ListOfDictTableModel(QAbstractTableModel):
    def __init__(self, rows=None, columns=None):
        super().__init__()
        self._rows = rows or []
        self._columns = columns or []

    def set_data(self, rows, columns=None):
        self.beginResetModel()
        self._rows = rows or []
        if columns is not None:
            self._columns = columns
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._rows)

    def columnCount(self, parent=QModelIndex()):
        if parent.isValid():
            return 0
        return len(self._columns)

    def data(self, index, role=Qt.DisplayRole):
        if not index.isValid():
            return None

        if role == Qt.DisplayRole:
            row = self._rows[index.row()]
            col = self._columns[index.column()]
            value = row.get(col, "")
            return "" if value is None else str(value)

        return None

    def headerData(self, section, orientation, role=Qt.DisplayRole):
        if role != Qt.DisplayRole:
            return None

        if orientation == Qt.Horizontal:
            if 0 <= section < len(self._columns):
                return self._columns[section]
            return None

        return str(section + 1)


class Worker(QObject):
    finished = Signal(object, object, object, object)  # csv_bytes, activity_rows, sleep_rows, date_range
    failed = Signal(str)

    def __init__(self, zip_path: str, participant_id: str, start_date, end_date, intersect_dates: bool):
        super().__init__()
        self.zip_path = zip_path
        self.participant_id = participant_id
        self.start_date = start_date
        self.end_date = end_date
        self.intersect_dates = intersect_dates

    @Slot()
    def run(self):
        try:
            zip_bytes = Path(self.zip_path).read_bytes()
            out_bytes, activity_rows, sleep_rows, date_range = convert_takeout_zip_bytes(
                zip_bytes,
                intersect_dates=self.intersect_dates,
                user_start=self.start_date,
                user_end=self.end_date,
            )
            self.finished.emit(out_bytes, activity_rows, sleep_rows, date_range)
        except Exception:
            self.failed.emit(traceback.format_exc())


class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Fitbit Takeout to CSV")
        self.resize(1100, 750)

        self.zip_path = ""
        self.csv_bytes = None
        self.current_range = None
        self.activity_rows = []
        self.sleep_rows = []

        central = QWidget()
        self.setCentralWidget(central)
        root = QVBoxLayout(central)

        form = QFormLayout()

        self.participant_edit = QLineEdit()
        self.participant_edit.setPlaceholderText("e.g. P001")

        self.start_date = QDateEdit()
        self.start_date.setCalendarPopup(True)
        self.start_date.setDate(QDate.currentDate().addMonths(-1))

        self.end_date = QDateEdit()
        self.end_date.setCalendarPopup(True)
        self.end_date.setDate(QDate.currentDate())

        self.intersect_checkbox = QCheckBox("Keep only intersection date range across domains")
        self.intersect_checkbox.setChecked(True)

        form.addRow("Participant ID", self.participant_edit)
        form.addRow("Start Date", self.start_date)
        form.addRow("Returned Date", self.end_date)
        form.addRow("", self.intersect_checkbox)

        root.addLayout(form)

        file_row = QHBoxLayout()
        self.file_label = QLabel("No ZIP selected")
        self.pick_button = QPushButton("Choose Takeout ZIP")
        self.pick_button.clicked.connect(self.choose_zip)
        file_row.addWidget(self.file_label, 1)
        file_row.addWidget(self.pick_button)
        root.addLayout(file_row)

        action_row = QHBoxLayout()
        self.process_button = QPushButton("Process")
        self.process_button.clicked.connect(self.process_zip)
        self.save_button = QPushButton("Save CSV")
        self.save_button.setEnabled(False)
        self.save_button.clicked.connect(self.save_csv)
        action_row.addWidget(self.process_button)
        action_row.addWidget(self.save_button)
        root.addLayout(action_row)

        self.status_label = QLabel("Ready")
        root.addWidget(self.status_label)

        self.tabs = QTabWidget()
        self.activity_table = QTableView()
        self.sleep_table = QTableView()

        self.activity_model = ListOfDictTableModel([], ACTIVITY_COLUMNS)
        self.sleep_model = ListOfDictTableModel([], SLEEP_COLUMNS)

        self.activity_table.setModel(self.activity_model)
        self.sleep_table.setModel(self.sleep_model)

        self.tabs.addTab(self.activity_table, "Activities")
        self.tabs.addTab(self.sleep_table, "Sleep")
        root.addWidget(self.tabs)

        self.thread = None
        self.worker = None

    def choose_zip(self):
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Choose Google Takeout ZIP",
            "",
            "ZIP Files (*.zip)",
        )
        if path:
            self.zip_path = path
            self.file_label.setText(path)

    def process_zip(self):
        if not self.zip_path:
            QMessageBox.warning(self, "Missing ZIP", "Please choose a Google Takeout ZIP file first.")
            return

        participant_id = self.participant_edit.text().strip()
        if not participant_id:
            QMessageBox.warning(self, "Missing Participant ID", "Please enter a Participant ID.")
            return

        start_py = self.start_date.date().toPython()
        end_py = self.end_date.date().toPython()
        if start_py > end_py:
            QMessageBox.warning(self, "Invalid Dates", "Start Date must be on or before Returned Date.")
            return

        self.process_button.setEnabled(False)
        self.save_button.setEnabled(False)
        self.status_label.setText("Processing...")

        self.thread = QThread()
        self.worker = Worker(
            self.zip_path,
            participant_id,
            start_py,
            end_py,
            self.intersect_checkbox.isChecked(),
        )
        self.worker.moveToThread(self.thread)

        self.thread.started.connect(self.worker.run)
        self.worker.finished.connect(self.on_finished)
        self.worker.failed.connect(self.on_failed)

        self.worker.finished.connect(self.thread.quit)
        self.worker.failed.connect(self.thread.quit)
        self.thread.finished.connect(self.thread.deleteLater)

        self.thread.start()

    def on_finished(self, csv_bytes, activity_rows, sleep_rows, date_range):
        self.csv_bytes = csv_bytes
        self.current_range = date_range
        self.activity_rows = activity_rows or []
        self.sleep_rows = sleep_rows or []

        self.activity_model.set_data(self.activity_rows[:50], ACTIVITY_COLUMNS)
        self.sleep_model.set_data(self.sleep_rows[:50], SLEEP_COLUMNS)

        self.activity_table.resizeColumnsToContents()
        self.sleep_table.resizeColumnsToContents()

        self.save_button.setEnabled(True)
        self.process_button.setEnabled(True)

        if date_range is not None:
            self.status_label.setText(f"Done. Effective range: {date_range[0]} to {date_range[1]}")
        else:
            self.status_label.setText("Done.")

    def on_failed(self, tb: str):
        self.process_button.setEnabled(True)
        self.save_button.setEnabled(False)
        self.status_label.setText("Failed.")
        QMessageBox.critical(self, "Processing Error", tb)

    def save_csv(self):
        if not self.csv_bytes:
            QMessageBox.warning(self, "No Output", "Please process a ZIP file first.")
            return

        pid = sanitize_filename_part(self.participant_edit.text())
        default_name = f"Fitbit_{pid}.csv"

        path, _ = QFileDialog.getSaveFileName(
            self,
            "Save Output CSV",
            default_name,
            "CSV Files (*.csv)",
        )
        if path:
            Path(path).write_bytes(self.csv_bytes)
            QMessageBox.information(self, "Saved", f"Saved to:\n{path}")


if __name__ == "__main__":
    app = QApplication([])
    w = MainWindow()
    w.show()
    app.exec()
