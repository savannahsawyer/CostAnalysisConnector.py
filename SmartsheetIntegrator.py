costAnalysisAPIKey = {
    'access_token': '3zolofrfk5ww2plarjdo5lku8y',
    'user_agent': 'McK Energy Services',
    'sheet_id': 5242159034066820, # https://app.smartsheet.com/sheets/fH76g8wQJG9RmGJR85HJ56WP5x5MGp8rXw7fqPp1
    'request_sheet_id': 340004930119556 # https://app.smartsheet.com/sheets/PRHxfHWMhPpF85r8GFxFPr5C7v3CwVHR59Jgj6G1
}

"""Smartsheet logic

Handles pulling and pushing data to Smartsheet.

"""

from smartsheet import Smartsheet
from smartsheet.sheets import Sheets
from smartsheet.models.sheet import Sheet as SmartsheetSheet
from smartsheet.models.cell import Cell as SmartsheetCell
from smartsheet.models.row import Row as SmartsheetRow
from smartsheet.models.column import Column as SmartsheetColumn
from DynamicsIntegrator import dynamicsCost, dynamicsProjectsCost, createCostAnalysisIdentifier


class smartsheetConnection:
    sharedConn = None
    @classmethod
    def getSharedConn(cls):
        if cls.sharedConn is None:
            cls.sharedConn = smartsheetConnection(costAnalysisAPIKey["access_token"], costAnalysisAPIKey["user_agent"])
        return cls.sharedConn

    def __init__(self, access_token, user_agent, appVersion=None):
        self.access_token = access_token
        self.user_agent = user_agent
        self.appVersion = appVersion
        self.apiModel = None
        self.sheets = None
        self.createConnection()

    def createConnection(self):
        self.apiModel = Smartsheet(access_token=self.access_token, user_agent=self.user_agent)
        self.apiModel.errors_as_exceptions(True)
        self.sheets = Sheets(self.apiModel)

    def getSheet(self, sheetID, page_size=5000, verbose = True):
        if verbose:
            print("Accessing sheet with ID '{0}'".format(sheetID))
        sheet = self.sheets.get_sheet(sheetID, page_size=page_size)
        if verbose:
            print("Sheet '{0}' accessed for Smartsheet".format(sheet.name))
        return sheet # create an object for this?



class projectAnalysisRequest:
    def __init__(self):
        self.rows = []
        self.column_map = {} # given a column name / title, find the ID
        self.sheet = None
        self.verbose = False

    def getRequestRows(self, forceUpdate=False):
        if len(self.rows) == 0 or forceUpdate:
            self.rows = []
            # pulls in the smartsheet and connection
            if self.sheet is None or forceUpdate:
                self.sheet = smartsheetConnection.getSharedConn().getSheet(costAnalysisAPIKey['request_sheet_id'], verbose=self.verbose)
                # since have gotten the new sheet, create the column lookup
                if len(self.column_map) == 0 or forceUpdate:
                    self.column_map = {}
                    assert isinstance(self.sheet, SmartsheetSheet), "Input sheet (type '{0}') is not the correct type".format(type(self.sheet))
                    for column in self.sheet.columns:
                        assert isinstance(column, SmartsheetColumn), "Input column (type '{0}') is not the correct type".format(type(column))
                        self.column_map[column.title] = column.id_

            # pull in the rows from the sheet
            assert isinstance(self.sheet, SmartsheetSheet), "Input sheet (type '{0}') is not the correct type".format(type(self.sheet))
            for currRow in self.sheet.rows:
                self.rows.append(projectAnalysisRequestRow(self, currRow))
        return self.rows

    def runUpdates(self):
        for currRow in self.rows:
            assert isinstance(currRow, projectAnalysisRequestRow)
            currRow.updateSmartsheetCostAnalysis()

class projectAnalysisRequestRow:
    def __init__(self, parent, smRow):
        assert isinstance(parent, projectAnalysisRequest)
        self.parent = parent
        self.row = smRow
        self.projectNumber = 0
        self.sheetId = 0
        self.hasAllParams = False
        self.pullInFromRow(smRow)

    def pullInFromRow(self, smRow):
        self.row = smRow
        self.projectNumber = self.getCellValueFromColumnName("Cleaned Project Number")
        self.sheetId = self.getCellValueFromColumnName("Sheet ID")
        self.hasAllParams = self.getCellValueFromColumnName("All Conditions Satisfied?")

    def getCellValueFromColumnName(self, columnName):
        associatedCell = self.row.get_column(self.parent.column_map[columnName])
        assert isinstance(associatedCell, SmartsheetCell), "Cell is not of correct type"
        cellVal = associatedCell.value
        if cellVal is None:
            return ""
        else:
            if isinstance(cellVal, float):
                if cellVal % 1 == 0:
                    return int(cellVal)
            return cellVal

    def updateSmartsheetCostAnalysis(self):
        if self.hasAllParams:
            # create an instance of the dynamcis handler
            # note that the instantiation automatically loads in the dynamics query results
            try:
                dyn_ca = dynamicsProjectsCost(self.projectNumber)

                # create an instance of the handler for the Smartsheet Cost Analysis
                # note that the instantiation automatically loads in the rows
                sm_ca = projectCostAnalysis(self.sheetId)

                # push the results of the dynamics cost analysis into the smartsheet
                sm_ca.updateSheetFromProjectCostAnalysis(dyn_ca)
            except Exception as e:
                print("There was a problem while pushing project '{0}' to Smartsheet with id '{1}': {2}".format(self.projectNumber, self.sheetId, e))






class projectCostAnalysis:
    updatesAtATime = 40
    def __init__(self, sheetId = None):
        self.smartsheetRowLookup = {}
        self.rowsToUpdate = []
        self.rowsToAdd = []
        self.rowsToDelete = []
        self.verbose = True
        self.enactUpdates = True
        self.costAnalysisSheet = None
        self.column_map = {} # given a column name / title, find the ID
        self.colIdLookup = {} # given a column ID, find the name / title
        self.colLookup = {} # given a column ID, find the column
        self.columnFormulaStatusLookup = {} # given a column ID, find if the column has a column formula specified
        self.sheetId = costAnalysisAPIKey["sheet_id"] if sheetId is None else sheetId
        self.getSmCostAnalysisRows(True)

    def getSmCostAnalysisRows(self, forceUpdate=False):
        if len(self.smartsheetRowLookup) == 0 or forceUpdate:
            self.smartsheetRowLookup = {}
            # means that there are no calls, so go ahead an pull
            self.getCostAnalysisSheet() # pulls in the smartsheet and connection
            assert isinstance(self.costAnalysisSheet, SmartsheetSheet), "Input warranty sheet (type '{0}') is not the correct type".format(type(self.costAnalysisSheet))
            for currRow in self.costAnalysisSheet.rows:
                newCall = costAnalysisRow(currRow, self)
                if newCall.hasData:
                    self.smartsheetRowLookup[newCall.id] = newCall
                else:
                    self.setRowForDeletion(newCall)
        return self.smartsheetRowLookup

    def getCostAnalysisSheet(self, forceUpdate=False):
        if self.costAnalysisSheet is None or forceUpdate:
            self.costAnalysisSheet = smartsheetConnection.getSharedConn().getSheet(self.sheetId, verbose=self.verbose)
            self.createColumnLookup(True)
        return self.costAnalysisSheet

    def createColumnLookup(self, forceUpdate = False):
        if len(self.column_map) == 0 or forceUpdate:
            self.column_map = {}
            self.colIdLookup = {}
            self.colLookup = {}
            assert isinstance(self.costAnalysisSheet, SmartsheetSheet), "Input warranty sheet (type '{0}') is not the correct type".format(type(self.costAnalysisSheet))
            for column in self.costAnalysisSheet.columns:
                assert isinstance(column, SmartsheetColumn), "Input warranty column (type '{0}') is not the correct type".format(type(column))
                self.column_map[column.title] = column.id_
                self.colIdLookup[column.id_] = column.title
                self.colLookup[column.id_] = column
                # column formula will be none if no column formula has been declared
                self.columnFormulaStatusLookup[column.id_] = column.formula is not None
        return self.column_map

    def getColumnById(self, col_id):
        return self.colLookup[col_id]

    def getColumnFormulaStatusById(self, col_id):
        return self.columnFormulaStatusLookup[col_id]

    def updateSheetFromProjectCostAnalysis(self, pca):
        assert isinstance(pca, dynamicsProjectsCost)
        # go through all of the existing smartsheet rows, and identify which rows need to be deleted and updated
        smRow: costAnalysisRow
        for sm_ca_Id, smRow in self.smartsheetRowLookup.items():
            if sm_ca_Id in pca.costAnalysisLookup:
                # means that need to update the Smartsheet row based on the information from the cost analysis
                smRow.updateRowFromDynamics(pca.costAnalysisLookup[sm_ca_Id])
            else:
                # means that doesn't exist in the cost analysis, so need to delete
                self.setRowForDeletion(smRow)

        # now go through all of the cost analysis entries to see which need to be added
        for ca_id, caItem in pca.costAnalysisLookup.items():
            if ca_id not in self.smartsheetRowLookup:
                # means that need to add the cost analysis item into the smartsheet, so add to the bottom
                self.createRowFromDynamicsEntry(caItem)

        # lastly, push the update
        self.pushUpdates()
                

    def pushUpdates(self):
        self.getCostAnalysisSheet() # ensure that have a sheet & connection
        if self.verbose:
            print("Initializing update for the dynamics rows")
        assert isinstance(smartsheetConnection.getSharedConn().sheets, Sheets)
        assert isinstance(self.costAnalysisSheet, SmartsheetSheet), "Input warranty sheet (type '{0}') is not the correct type".format(type(self.costAnalysisSheet))

        if len(self.rowsToUpdate) == 0 and len(self.rowsToAdd) == 0 and len(self.rowsToDelete) == 0:
            if self.verbose:
                print("No changes were required.")
        else:
            if self.verbose:
                print("Updated {0} rows, added {1} rows, and deleted {2} rows".format(len(self.rowsToUpdate),
                                                                                      len(self.rowsToAdd),
                                                                                      len(self.rowsToDelete)))
            if len(self.rowsToUpdate) > 0:
                if self.enactUpdates:
                    for i in range(0, len(self.rowsToUpdate), self.updatesAtATime):
                        smartsheetConnection.getSharedConn().sheets.update_rows(self.costAnalysisSheet.id_, self.rowsToUpdate[i:i+self.updatesAtATime])
                self.rowsToUpdate = []
            if len(self.rowsToAdd) > 0:
                if self.enactUpdates:
                    for i in range(0, len(self.rowsToAdd), self.updatesAtATime):
                        self.costAnalysisSheet.add_rows(self.rowsToAdd[i:i+self.updatesAtATime])
                self.rowsToAdd = []
            if len(self.rowsToDelete) > 0:
                if self.enactUpdates:
                    for i in range(0, len(self.rowsToDelete), self.updatesAtATime):
                        self.costAnalysisSheet.delete_rows(self.rowsToDelete[i:i+self.updatesAtATime])
                self.rowsToDelete = []

    def createNewCell(self, colName, cellVal, strict=False, formula=None):
        assert colName in self.column_map, "An unknown column title of '{0}' was used to create a cell".format(colName)
        colId = self.column_map[colName]
        # check to make sure that the column doesn't have a column formula
        if self.getColumnFormulaStatusById(colId):
            return None  # don't create a new cell, because is a column formula (and will be created automatically)

        newCostCell = SmartsheetCell()
        newCostCell.column_id = colId
        if formula is None:
            newCostCell.value = cellVal
        else:
            newCostCell.formula = formula
        newCostCell.strict = strict
        return newCostCell

    def createRowFromDynamicsEntry(self, dynEntry):
        assert isinstance(dynEntry, dynamicsCost)
        rowCells = [
            self.createNewCell(costAnalysisRow.projectIdAttr, dynEntry.projectId),
            self.createNewCell(costAnalysisRow.taskCodeAttr, dynEntry.taskId),
            self.createNewCell(costAnalysisRow.acctTypeAttr, dynEntry.acctType),
            self.createNewCell(costAnalysisRow.taskDescAttr, dynEntry.taskDesc),
            self.createNewCell(costAnalysisRow.revAmtAttr, dynEntry.revised.amount),
            self.createNewCell(costAnalysisRow.projAmtAttr, dynEntry.projected.amount),
            self.createNewCell(costAnalysisRow.committedAmtAttr, dynEntry.committedAmt),
            self.createNewCell(costAnalysisRow.actualAmtAttr, dynEntry.actual.amount),
            self.createNewCell(costAnalysisRow.etcAmtAttr, dynEntry.etc.amount),
            self.createNewCell(costAnalysisRow.revHrsAttr, dynEntry.revised.units),
            self.createNewCell(costAnalysisRow.projHrsAttr, dynEntry.projected.units),
            self.createNewCell(costAnalysisRow.acutalHrsAttr, dynEntry.actual.units),
            self.createNewCell(costAnalysisRow.etcHrsAttr, dynEntry.etc.units),
            self.createNewCell(costAnalysisRow.codeTypeLookupAttr,
                               costAnalysisRow.getCodeTypeLookup(dynEntry.taskId, dynEntry.acctType)),
            self.createNewCell(costAnalysisRow.craftAttr, costAnalysisRow.getCraft(dynEntry.taskId)),
            self.createNewCell(costAnalysisRow.checkHrsAttr, "", formula=costAnalysisRow.generateCheckHrsFormula())
        ]
        rowCells = [c for c in rowCells if not c is None] # take out any cells where the item is none (i.e., cell should not be created)
        # make Row object
        row = SmartsheetRow(props={
            'cells': rowCells,
            'toBottom': True,
            'format': ',,,,,,,,,3,,,,,,'  # blank format
        })
        self.rowsToAdd.append(row)
        if self.verbose:
            print("Created a new row - Project: {0}, Task: {1}, Acct Type: {2}".format(dynEntry.projectId,
                                                                                       dynEntry.taskId,
                                                                                       dynEntry.acctType))
        return row

    def setRowForDeletion(self, smRow):
        """
        Sets up a Smartsheet row to be deleted. Note that must call the update function after to perform deletion
        :type smRow: costAnalysisRow
        """
        if self.verbose:
            print("Deleted row - Project: {0}, Task: {1}, Acct Type: {2} (row #{3})".format(smRow.projectId,
                                                                                            smRow.taskId,
                                                                                            smRow.acctType,
                                                                                            smRow.sourceRow.row_number))
        self.rowsToDelete.append(smRow.sourceRow.id_)

    def setRowUpdates(self, smRow):
        """
        Sets up a Smartsheet row to be updated. Note that must call the update function after to perform update
        :type smRow: costAnalysisRow
        """
        if len(smRow.cellsToUpdate) > 0:
            # changes are needed, so log the changes, and send off
            rowWithUpdateInfo = SmartsheetRow()
            rowWithUpdateInfo.id = smRow.sourceRow.id
            for currCell in smRow.cellsToUpdate:
                rowWithUpdateInfo.cells.append(currCell)
            self.rowsToUpdate.append(rowWithUpdateInfo)
            if self.verbose:
                strFormat = "Updated row - Project: {0}, Task: {1}, Acct Type: {2} (row #{3}) on fields: {4}"
                print(strFormat.format(smRow.projectId, smRow.taskId, smRow.acctType, smRow.sourceRow.row_number,
                                       ", ".join([self.colIdLookup[c.column_id] for c in smRow.cellsToUpdate])))
            smRow.cellsToUpdate = []
            return True  # means that an update was requested
        else:
            return False  # means that an update was not requested


class costAnalysisRow:
    sourceRow: SmartsheetRow
    projectIdAttr = "ProjectID"
    taskCodeAttr = "Task Code"
    acctTypeAttr = "Acct"
    taskDescAttr = "Task Description"
    revAmtAttr = "Rev Amt"
    projAmtAttr = "Proj Amt"
    committedAmtAttr = "Committed Amt"
    actualAmtAttr = "Actual Amt"
    etcAmtAttr = "ETC Amt"
    revHrsAttr = "Rev Hrs"
    projHrsAttr = "Proj Hrs"
    acutalHrsAttr = "Actual Hrs"
    etcHrsAttr = "ETC Hrs"
    codeTypeLookupAttr = "Code & Type Lookup"
    craftAttr = "Craft"
    checkHrsAttr = "Check Hrs"

    @classmethod
    def getCodeTypeLookup(cls, taskCode, laborType):
        return str(taskCode) + laborType[0]

    @classmethod
    def getCraft(cls, taskCode):
        craftDigit = str(taskCode)[0]
        if craftDigit.isnumeric():
            return int(craftDigit)
        else:
            return craftDigit

    @classmethod
    def getFormulaColNam(cls, colName):
        if " " in colName:
            return "[" + colName + "]"
        else:
            return colName

    @classmethod
    def generateCheckHrsFormula(cls, rowNum="@row"):
        """
        Generates the formula for checking the hours of a given entry
        Note that if don't know the row number (like if adding row), then can use the @row notation
        :type rowNum: int, str
        """
        templateStr = '=IF(AND({projHrs}{rowNum} <> 0, LEFT({acctType}{rowNum}, 5) = "LABOR"), SUMIFS({projHrs}:{projHrs}, {taskCode}:{taskCode}, {taskCode}{rowNum}) - SUMIF({costCodeRange}, {taskCode}{rowNum}, {taskHrsRange}))'
        return templateStr.format(projHrs=cls.getFormulaColNam(cls.projHrsAttr),
                                  rowNum=rowNum,
                                  acctType=cls.getFormulaColNam(cls.acctTypeAttr),
                                  taskCode=cls.getFormulaColNam(cls.taskCodeAttr),
                                  costCodeRange="{Cost Code}",
                                  taskHrsRange="{Task Hrs}")

    def __init__(self, sourceRow, parent):
        assert isinstance(parent, projectCostAnalysis), "Parent is of incorrect type."
        self.sourceRow = sourceRow
        self.parent = parent
        self.projectId = self.getCellValueFromColumnName(self.projectIdAttr)
        self.taskId = self.getCellValueFromColumnName(self.taskCodeAttr)
        self.acctType = self.getCellValueFromColumnName(self.acctTypeAttr)
        self.id = createCostAnalysisIdentifier(self.projectId, self.taskId, self.acctType)
        self.hasData = self.projectId != "" or self.taskId != "" or self.acctType != ""
        self.cellsToUpdate = []

    def getCellByColumnName(self, columnName):
        colID = self.parent.column_map[columnName]
        return self.sourceRow.get_column(colID)

    def getCellValueFromColumnName(self, columnName):
        associatedCell = self.getCellByColumnName(columnName)
        assert isinstance(associatedCell, SmartsheetCell), "Cell is not of correct type"
        cellVal = associatedCell.value
        if cellVal is None:
            return ""
        else:
            if isinstance(cellVal, float):
                if cellVal % 1 == 0:
                    return int(cellVal)
            return cellVal

    def checkIfCellNeedsToBeUpdated(self, colName, newValue, newFormula = None):
        if newFormula is None:
            currValue = self.getCellValueFromColumnName(colName)
            if currValue != newValue:
                self.cellsToUpdate.append(self.parent.createNewCell(colName, newValue))
        else:
            currCell = self.getCellByColumnName(colName)
            assert isinstance(currCell, SmartsheetCell), "Cell is not of correct type"
            currFormula = currCell.formula
            if currFormula != newFormula:
                # since is not the same, check to see if the column already has a formula. If it does, don't update
                if not self.parent.getColumnFormulaStatusById(currCell.column_id): # will be none if no column formula has been declared
                    self.cellsToUpdate.append(self.parent.createNewCell(colName, "", formula=newFormula))

    def updateRowFromDynamics(self, dynEntry):
        assert isinstance(dynEntry, dynamicsCost)
        # note that the project ID, task code, and account type should all be the same (as this is how the lookup
        # was performed). Thus, just need to check all of the other attributes.
        if len(self.cellsToUpdate) > 0:
            self.cellsToUpdate = []

        # perform all of the checks to ensure that the values are properly updated
        # note that this ensures that only the changed information is pushed
        self.checkIfCellNeedsToBeUpdated(self.taskDescAttr, dynEntry.taskDesc)
        self.checkIfCellNeedsToBeUpdated(self.revAmtAttr, dynEntry.revised.amount)
        self.checkIfCellNeedsToBeUpdated(self.projAmtAttr, dynEntry.projected.amount)
        self.checkIfCellNeedsToBeUpdated(self.committedAmtAttr, dynEntry.committedAmt)
        self.checkIfCellNeedsToBeUpdated(self.actualAmtAttr, dynEntry.actual.amount)
        self.checkIfCellNeedsToBeUpdated(self.etcAmtAttr, dynEntry.etc.amount)
        self.checkIfCellNeedsToBeUpdated(self.revHrsAttr, dynEntry.revised.units)
        self.checkIfCellNeedsToBeUpdated(self.projHrsAttr, dynEntry.projected.units)
        self.checkIfCellNeedsToBeUpdated(self.acutalHrsAttr, dynEntry.actual.units)
        self.checkIfCellNeedsToBeUpdated(self.etcHrsAttr, dynEntry.etc.units)
        self.checkIfCellNeedsToBeUpdated(self.codeTypeLookupAttr,
                                         costAnalysisRow.getCodeTypeLookup(self.taskId, self.acctType))
        self.checkIfCellNeedsToBeUpdated(self.craftAttr, costAnalysisRow.getCraft(self.taskId))
        self.checkIfCellNeedsToBeUpdated(self.checkHrsAttr, "", self.generateCheckHrsFormula()) #self.sourceRow.row_number))
        self.parent.setRowUpdates(self)

