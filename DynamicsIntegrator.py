"""Database logic
Handles getting data from DYNAMICSSLAPP view
"""

import pyodbc

##################################
# Database settings
##################################

sql = {
    'connection': 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=MCKATLSQL;PORT=1433;DATABASE=MWA;UID=ActiveProjectCAUser;PWD=jobDATA@2020!',
    'query': """
        SELECT * FROM [MWA].[Queries].[CostAnalysis_ActiveProjects]
    """,
    'project_query': """
        SELECT * FROM [MWA].[Queries].[CostAnalysis_ActiveProjects] WHERE Project = '{0}'
    """
}

def dynamicsEntryToNumber(dynamicsEntry):
    """
    Converts the text representation of a number (with possible preceeding / following spaces) to a number
    :type dynamicsEntry: str
    """
    if isinstance(dynamicsEntry, int) or isinstance(dynamicsEntry, float):
        return dynamicsEntry
    else:
        return float(dynamicsEntry.strip().replace(",", ""))

def convertStrToDynamicsStr(item, numChars=-1):
    """
    Converts a given item into the appropriate Dynamics representation.
    If the specified number of characters is greater than 0, then it will pad with spaces to get to the desired length
    :type item: object
    """
    if numChars > 0:
        return str(item).ljust(numChars)
    else:
        if isinstance(item, float):
            if item % 1 == 0:
                return str(int(item))
        return str(item)

def createCostAnalysisIdentifier(projectNum, taskId, acctType):
    return "{0} {1} {2}".format(convertStrToDynamicsStr(projectNum),
                                convertStrToDynamicsStr(taskId),
                                convertStrToDynamicsStr(acctType))


class dynamicsProjectsCost:
    # class which contains a list of projects and their cost analysis
    def __init__(self, projectNo = None):
        self.projectLookup = {}
        self.costAnalysisLookup = {}
        self.numberCostsPulled = 0
        self.pullInProjects(projectNo)

    def pullInProjects(self, projectNo = None):
        # make a query to the Dynamics database, and pull down the results of the query
        print("Pulling the most recent cost analysis data from Dynamics...")
        sql_connection = pyodbc.connect(sql.get('connection'))  # Connection to SQL Server
        cursor = sql_connection.cursor()  # get Cursor object using the connection
        queryToRun = sql.get("query") if projectNo is None else sql.get("project_query").format(
            convertStrToDynamicsStr(projectNo, 16)) # note that Dynamics stores project numbers in a 16-char code that
            # starts with the project number (usually 8 characters) and the remainder is spaces
        cursor.execute(queryToRun)
        for row in cursor:
            self.pullInTransaction(row)
        print("Completed pulling data from Dynamics, with {0} results found".format(self.numberCostsPulled))

    def pullInTransaction(self, costRow):
        newCostRow = dynamicsCost(costRow)
        if newCostRow.isCost:
            # add to the project lookup
            if newCostRow.projectId not in self.projectLookup:
                self.projectLookup[newCostRow.projectId] = {}
            projectDict = self.projectLookup[newCostRow.projectId]
            if newCostRow.id in projectDict:
                # means that already exists, so add in
                existingEntry = projectDict[newCostRow.id]
                assert isinstance(existingEntry, dynamicsCost)
                existingEntry.mergeFromAnotherCostRow(newCostRow)
            else:
                # means that doesn't exist yet, so add
                projectDict[newCostRow.id] = newCostRow
                self.numberCostsPulled += 1

            # add to the cost analysis individual item lookup
            if newCostRow.id in self.costAnalysisLookup:
                # means that already exists, so add in
                existingEntry = self.costAnalysisLookup[newCostRow.id]
                assert isinstance(existingEntry, dynamicsCost)
                existingEntry.mergeFromAnotherCostRow(newCostRow)
            else:
                # means that doesn't exist yet, so add
                self.costAnalysisLookup[newCostRow.id] = newCostRow


class dynamicsUnitsAndAmount:
    # class for the easy containment of units (number of charges) and amount (cost of charges)
    # used for the projected / revised / actual / etc for each dynamics cost row
    def __init__(self, amount, units):
        self.units = dynamicsEntryToNumber(units) # this is the number of items that have been units
        self.amount = dynamicsEntryToNumber(amount) # this is the total cost of all of the units

    def __str__(self):
        return "Units: {0}\tCost: ${1}".format(self.units, self.amount)

    def mergeFromAnotherCostRow(self, otherUnitsAndAmount):
        """
        Imports (adds) information from another units & amounts object
        :type otherUnitsAndAmount: dynamicsUnitsAndAmount
        """
        # begin adding in the costs
        self.units += otherUnitsAndAmount.units
        self.amount += otherUnitsAndAmount.amount


class dynamicsCost:
    # class for an individual row of costs, identified by project, task code, and accounting type

    # provide a list of cost types that actually represent costs hitting a job
    projectCostTypes = ["LABOR", "LABOROT", "MATERIALS", "SUBCONTRACT", "PTI"]
    # provide a mappings that takes the key and transforms it to the value.
    projectCostMappings = {"LABOROT": "LABOR"}
    # provide a default item
    defaultUnitsAndAmount = dynamicsUnitsAndAmount(0, 0)

    def __init__(self, sqlRow):
        self.dataRow = sqlRow
        self.projectId = ""
        self.taskId = ""
        self.id = ""
        self.acctType = ""  # from AccountID
        self.isCost = False
        self.taskDesc = ""
        self.revised = self.defaultUnitsAndAmount
        self.projected = self.defaultUnitsAndAmount
        self.committedAmt = 0
        self.actual = self.defaultUnitsAndAmount
        self.etc = self.defaultUnitsAndAmount
        self.pullInFromSQLRow(sqlRow)

    def pullInFromSQLRow(self, sqlRow):
        self.projectId = sqlRow[0].strip()
        self.taskId = sqlRow[1].strip()
        self.setAccountingType(sqlRow[2].strip())
        self.id = createCostAnalysisIdentifier(self.projectId, self.taskId, self.acctType)
        self.taskDesc = sqlRow[3].strip()
        self.revised = dynamicsUnitsAndAmount(sqlRow[4], sqlRow[9])
        self.projected = dynamicsUnitsAndAmount(sqlRow[5], sqlRow[10])
        self.committedAmt = dynamicsEntryToNumber(sqlRow[6])
        self.actual = dynamicsUnitsAndAmount(sqlRow[7], sqlRow[11])
        self.etc = dynamicsUnitsAndAmount(sqlRow[8], sqlRow[12])

    def mergeFromAnotherCostRow(self, otherCostRow):
        assert isinstance(otherCostRow, dynamicsCost)
        assert otherCostRow.projectId == self.projectId
        assert otherCostRow.taskId == self.taskId
        assert otherCostRow.acctType == self.acctType
        # begin adding in the costs
        self.committedAmt += otherCostRow.committedAmt
        self.revised.mergeFromAnotherCostRow(otherCostRow.revised)
        self.projected.mergeFromAnotherCostRow(otherCostRow.projected)
        self.actual.mergeFromAnotherCostRow(otherCostRow.actual)
        self.etc.mergeFromAnotherCostRow(otherCostRow.etc)

    def setAccountingType(self, currAccountingType):
        if currAccountingType in self.projectCostTypes:
            self.isCost = True
            # must be within the predetermined cost types, so that don't pull in revenue or projections
            if currAccountingType in self.projectCostMappings:
                # if there is a mapping (for example, LABOROT rolling up to LABOR), then return the mapping
                self.acctType = self.projectCostMappings[currAccountingType]
            else:
                self.acctType = currAccountingType  # return the item
        else:
            self.isCost = False
            self.acctType = ""  # since isn't a true cost, then don't set

    def __str__(self):
        return "{0}\t{1}\tType: {2}\tUnits: {3}\tCost: ${4}".format(self.projectId, self.taskId, self.acctType,
                                                                    self.actual.units, self.actual.amount)




if __name__ == "__main__":
    costHanlder = dynamicsProjectsCost(13601588)

