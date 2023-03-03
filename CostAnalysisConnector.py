from SmartsheetIntegrator import projectAnalysisRequest
import logging

def logger():
    out = r'\\mckatlauto2\Users\rpasvc1\Documents\Lloyd\ProjectTasksSmartsheetConnector\app.log'
    logging.basicConfig(filename=out, filemode='w', format='%(name)s - %(levelname)s - %(message)s')
    logging.warning('Success')
if __name__ == "__main__":
    # code run if just this file is called from a script (not imported as a module in Python)
    requestHandler = projectAnalysisRequest()
    requestHandler.getRequestRows()
    requestHandler.runUpdates()
    logger()


