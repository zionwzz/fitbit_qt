from PySide6.QtCore import QAbstractTableModel, QModelIndex, Qt


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
