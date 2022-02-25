# -*- coding: utf-8 -*-
"""
Created on Wed Oct  6 10:42:24 2021

@author: nmadmin2
"""
# %% Packages
import pandas as pd
import pyodbc

# %% Establish Database connection
RKH_DB23 = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=BI-PROD-DB01,23502;DATABASE=RKH_LIVE;Trusted_Connection=yes')
LUX_DB23 = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=BI-DEV-DB23;DATABASE=LUX_LIVE_10042021;Trusted_Connection=yes')

LUX_DB23.setdecoding(pyodbc.SQL_CHAR, encoding='latin1')
LUX_DB23.setencoding('latin1')
# cursor for connections
RKH_cursor = RKH_DB23.cursor()
LUX_cursor = LUX_DB23.cursor()
# %% Creat a class that includes variable that reads sql query
class SQL_Extract_Slips:
    LUX_query =  pd.read_sql_query(
"""
WITH DivisionPath
AS (SELECT
        Div.SettingID
      , Div.DivisionID
      , Div.Version
      , [LibraryPath] = '\\' + PthCalc.ServerName + '\' + REPLACE(Div.WorkSpace, '/', '\') + '\'
                        + Div.DocumentDocLibrary + '\' + Div.DocumentPath
      , [LibraryURL]  = Div.Server + '/' + Div.WorkSpace + '/' + Div.DocumentDocLibrary + '/' + Div.DocumentPath
      , Div.Server
      , ServerName    = PthCalc.ServerName
      , Div.WorkSpace
      , SiteName      = PthCalc.SiteName
      , Div.DocumentDocLibrary
      , Div.DocumentPath
      , Div.CoordinatorName
      , Div.CoordinatorPassword
      , Div.TemplatePath
      , Div.ReportPath
      , Div.UseCurrencySpecialSymbol
      , Div.DebitTextAfterValue
      , Div.CreditTextAfterValue
      , Div.TemplateDocLibrary
      , Div.ReportDocLibrary
      , Div.InboxRegPath
      , Div.DatabaseName
      , Div.SpsDbSettingsEncrypted
      , Div.VersionDate
      , Div.IsUseSpecialAccount
      , Div.SpecialAccountUserName
      , Div.SpecialAccountPassword
      , Div.DocumentStorageSystem
    FROM
        dbo.todmDivisions AS Div
        CROSS APPLY
    (
        SELECT
            ServerName = REPLACE(Div.Server, 'http://', '')
          , SiteName   = REPLACE(Div.WorkSpace, 'sites/', '')
    )                     AS PthCalc )
    , ContractObjects
AS (SELECT
        ObjectID          = Facility.FacilityID
      , BusinessObject.ObjectCode
      , BusinessObject.ObjectName
      , Reference         = Facility.FacilityRef
      , Facility.UniqueMarketRef
      , Facility.InceptionDate
      , Facility.ExpiryDate
      , Facility.CreateDate
      , Department.DepartmentID
      , Department.DepartmentName
      , Facility.SubDepartmentID
      , SubDepartmentName = SubDepartment.Name
      , Facility.BrokerID
    FROM
        dbo.toFacility                   AS Facility
        INNER JOIN dbo.toFacilityTypes   AS FacilityType
            ON FacilityType.FacilityTypesID = Facility.TypeFacility
        INNER JOIN dbo.toBusinessObjects AS BusinessObject
            ON BusinessObject.ObjectCode = FacilityType.Code
        INNER JOIN dbo.toDepartments     AS Department
            ON Department.DepartmentID = Facility.DepartmentID
        INNER JOIN dbo.toSubDepartments  AS SubDepartment
            ON SubDepartment.SubDepartmentID = Facility.SubDepartmentID
    UNION ALL
    SELECT
        ObjectID          = MasterPolicy.PolicyID
      , BusinessObject.ObjectCode
      , BusinessObject.ObjectName
      , Reference         = MasterPolicy.PolicyRef
      , MasterPolicy.UniqueMarketRef
      , MasterPolicy.InceptionDate
      , MasterPolicy.ExpiryDate
      , MasterPolicy.CreateDate
      , Department.DepartmentID
      , Department.DepartmentName
      , MasterPolicy.SubDepartmentID
      , SubDepartmentName = SubDepartment.Name
      , MasterPolicy.BrokerProducerID
    FROM
        dbo.toMasterPolicy               AS MasterPolicy
        INNER JOIN dbo.toDepartments     AS Department
            ON Department.DepartmentID = MasterPolicy.DepartmentID
        INNER JOIN dbo.toBusinessObjects AS BusinessObject
            ON BusinessObject.ObjectCode = 'P'
        INNER JOIN dbo.toSubDepartments  AS SubDepartment
            ON SubDepartment.SubDepartmentID = MasterPolicy.SubDepartmentID)
    , DocumentPrioritised
AS (SELECT
        Doc.DocumentID
      , Fld.FolderType
      , Obj.UniqueMarketRef
      , Obj.Reference
      , Obj.InceptionDate
      , BusObj.ObjectName
      , Doc.RootObjectID
      , Departments.DepartmentName
      , Departments.DepartmentID
      , Obj.SubDepartmentName
      , Pth.ServerName
      , Pth.SiteName
      , Doc.FolderID
      , Doc.DocumentName
      , DocumentTypePriorityOrdinal = DocumentTypePriority.Ordinal
      , DocumentPriorityOrdinal     = ROW_NUMBER() OVER (PARTITION BY
                                                              Doc.RootObjectID
                                                            , Doc.RootObjectCode
                                                          ORDER BY DocumentTypePriority.Ordinal ASC
                                                                , Doc.CreateDate DESC
                                                        )
      , Document.DocumentFileType
      , Document.DocumentFilePath
      , Document.DocumentFileURL
      , Pth.LibraryPath
      , FolderCalc.FolderPath
      , [FileName]                  = STUFF(Doc.FileName, 1, 1, '')
      , Doc.SettingsVersion
      , Doc.TemplateID
      , Doc.DocNumber
      , Doc.CreateDate
      , Doc.UserName
    FROM
        dbo.todmDocuments                AS Doc
        INNER JOIN dbo.todmFolders       AS Fld
            ON Fld.FolderID = Doc.FolderID
        INNER JOIN DivisionPath          AS Pth
            ON Pth.DivisionID = Doc.DepartmentID
                AND ISNULL(Pth.Version, 0) = ISNULL(Doc.SettingsVersion, 0)
		INNER JOIN dbo.todmDocumentSystemStatuses AS DocSysSt
 			ON DocSysSt.DocumentSystemStatusID = Doc.DocumentSystemStatusID
        INNER JOIN dbo.toBusinessObjects AS BusObj
            ON BusObj.ObjectCode = Doc.RootObjectCode
        INNER JOIN dbo.toDepartments     AS Departments
            ON Departments.DepartmentID = Doc.DepartmentID
        INNER JOIN ContractObjects       AS Obj
            ON Obj.ObjectID = Doc.RootObjectID
                AND Obj.ObjectCode = BusObj.ObjectCode
        CROSS APPLY
    (
        SELECT
            FolderPath = COALESCE(
                                      REPLACE(NULLIF(Fld.FolderPath, ''), '/', '\')
                                    , '\' + Fld.SectionKey + '\' + Fld.FolderType
                                  )
          , FolderURL  = COALESCE(NULLIF(Fld.FolderPath, ''), '/' + Fld.SectionKey + '/' + Fld.FolderType)
    )                                    AS FolderCalc
        CROSS APPLY
    (
        SELECT
            DocumentFilePath = CONCAT(
                                          Pth.LibraryPath
                                        , FolderCalc.FolderPath
                                        , '_' + CAST(NULLIF(Doc.FolderNumber, 0) AS VARCHAR(2))
                                        , REPLACE(Doc.FileName, '/', '\')
                                      )
          , DocumentFileURL  = CONCAT(
                                          Pth.LibraryURL
                                        , FolderCalc.FolderURL
                                        , '_' + CAST(NULLIF(Doc.FolderNumber, 0) AS VARCHAR(2))
                                        , Doc.FileName
                                      )
          , DocumentFileType = CASE
                                    WHEN Doc.FileName LIKE '%.%' THEN
                                        REVERSE(LEFT(REVERSE(Doc.FileName), CHARINDEX('.', REVERSE(Doc.FileName)) - 1))
                                    ELSE
                                        NULL
                                END
    ) AS Document
        /*
------------------------------------------------------------------------
Document Priority: select a document according to the prioroty listed
------------------------------------------------------------------------
Most recent Placing word doc that contains slip in the name
Most recent signed word doc that contains slip in the name
Most recent Placing pdf that contains slip in the name
Most recent signed pdf that contains slip in the name
Most recent Placing word doc
Most recent signed word doc
Most recent placing pdf
Most recent signed pdf
Most Recent Placing slip
Most Recent Signed Slip
Most Recent Quote Slip
*/
        -- 
        CROSS APPLY
    (
        SELECT
            Ordinal = CASE
            		  WHEN Fld.FolderTypeID in ( 60, 97 )-- Signed and signing slip
                                AND Doc.DocumentName LIKE '%Slip%'
							   OR  Doc.DocumentName LIKE '%MRC%'
							   OR  Doc.DocumentName LIKE '%Contract%'
							   OR  Doc.DocumentName LIKE '%London Market%'
							   OR  Doc.DocumentName LIKE '%Final%'
							   --OR Doc.DocumentName IN (select PolicyRef from dbo.toMasterPolicy)
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
 							  1
 						 --WHEN Fld.FolderTypeID in ( 60, 97 )-- Signed and signing slip
 							--   AND  Doc.DocumentName IN (select PolicyRef from dbo.toMasterPolicy) THEN
 							--  2
 						 WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                                AND Doc.DocumentName LIKE '%Sign%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              3
					     WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                                AND Doc.DocumentName LIKE '%Scan%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              4
 						 WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                                AND Doc.DocumentName LIKE '%FOS%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              5
 						 WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                                AND Doc.DocumentName LIKE '%Policy%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              6
                          WHEN Fld.FolderTypeID = 44 -- Placing slip
                                AND Doc.DocumentName LIKE '%Slip%'
							   OR  Doc.DocumentName LIKE '%MRC%'
							   OR  Doc.DocumentName LIKE '%Contract%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              7
						  --WHEN Fld.FolderTypeID = 44 -- Placing slip
							 --  AND  Doc.DocumentName IN (select PolicyRef from dbo.toMasterPolicy) THEN
							 -- 8
                          WHEN Fld.FolderTypeID = 44 -- Placing slip
							   AND Doc.DocumentName LIKE '%Sign%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              9
						  WHEN Fld.FolderTypeID = 44 -- Placing slip
							   AND Doc.DocumentName LIKE '%Scan%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              10
						  WHEN Fld.FolderTypeID = 44 -- Placing slip
                                AND Doc.DocumentName LIKE '%FOS%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              11
						  WHEN Fld.FolderTypeID = 44 -- Placing Slip
                                AND Doc.DocumentName LIKE '%Policy%'
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              12
						  WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              13
                          WHEN Fld.FolderTypeID = 44 -- Placing slip
                                AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              14

                          ELSE
                              99
                      END
    ) AS DocumentTypePriority
    WHERE 1 = 1
          AND Fld.FolderTypeID IN ( 44, 52, 60, 83, 97 )
		  AND DocSysSt.StatusName = 'Exists' -- Only inlcude docments that currently exist
		  -- Date created
          AND Obj.InceptionDate > '2021-07-01'
		  AND Doc.CreateDate < '2022-02-15'
    )
SELECT
    DocPriority.DocumentID
  , DocPriority.FolderType
  , DocPriority.UniqueMarketRef
  , DocPriority.RootObjectID
  , DocPriority.Reference
  , DocPriority.InceptionDate
  , DocPriority.ObjectName
  , DocPriority.DepartmentID
  , DocPriority.DepartmentName
  , DocPriority.SubDepartmentName
  , DocPriority.ServerName
  , DocPriority.SiteName
  , DocPriority.FolderID
  , DocPriority.DocumentTypePriorityOrdinal
  , DocPriority.DocumentPriorityOrdinal
  , DocPriority.DocumentName
  , DocPriority.DocumentFileType
  , DocPriority.DocumentFilePath
  , DocPriority.DocumentFileURL
  , DocPriority.LibraryPath
  , DocPriority.FolderPath
  , DocPriority.FileName
  , DocPriority.DocNumber
  , DocPriority.CreateDate
FROM
    DocumentPrioritised AS DocPriority
WHERE 1 = 1
      AND DocPriority.DocumentTypePriorityOrdinal  = 1
 	  AND DocPriority.DocumentFileType = 'pdf'
 	  --AND DocPriority.UniqueMarketRef in ( )
    and DocPriority.DocumentName 
		NOT LIKE '%Note%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Bind%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Binder%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Cert%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Certificate%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Invoices%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Withhold%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Wording%' 
 	and DocPriority.DocumentName 
		NOT LIKE '%Debit%'
 	and DocPriority.DocumentName 
		NOT LIKE '%DebitNote%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Credit%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Note%'
 	and DocPriority.DocumentName 
		NOT LIKE '%CoverNote%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Quote%'
 	and DocPriority.DocumentName 
		NOT LIKE '%Client%'
 	and DocPriority.DocumentName 
		NOT LIKE '%cc%'
 	and DocPriority.DocumentName 
		NOT LIKE '%cv%'
 	and DocPriority.DocumentName
		NOT LIKE '%subscription%'
		and DocPriority.DocumentName
		NOT LIKE '%FON%'
 	AND DocPriority.DocumentName
		NOT LIKE '%Endt%'
 	AND DocPriority.DocumentName
		NOT LIKE '%Endors%'
 	AND DocPriority.DocumentName
		NOT LIKE '%Placing%'
 	AND DocPriority.DocumentName
		NOT LIKE '%Amend%'
 	AND DocPriority.DocumentName
		NOT LIKE '%Excess%'
 	AND DocPriority.DocumentName
		NOT LIKE '%xs%'
 	AND DocPriority.DocumentName
		NOT LIKE '%dec%'
 	AND DocPriority.DocumentName
		NOT LIKE '%schedule%'
 	AND DocPriority.DocumentName
		NOT LIKE '%cover%'
 	AND DocPriority.DocumentName
		NOT LIKE '%consort%'
ORDER BY DocPriority.UniqueMarketRef DESC
        , DocPriority.DocumentPriorityOrdinal ASC;
""", LUX_DB23)


    RKH_query =  pd.read_sql_query(
"""
WITH DivisionPath
AS (SELECT
        Div.SettingID
      , Div.DivisionID
      , Div.Version
      , [LibraryPath] = '\\' + PthCalc.ServerName + '\' + REPLACE(Div.WorkSpace, '/', '\') + '\'
                        + Div.DocumentDocLibrary + '\' + Div.DocumentPath
      , [LibraryURL]  = Div.Server + '/' + Div.WorkSpace + '/' + Div.DocumentDocLibrary + '/' + Div.DocumentPath
      , Div.Server
      , ServerName    = PthCalc.ServerName
      , Div.WorkSpace
      , SiteName      = PthCalc.SiteName
      , Div.DocumentDocLibrary
      , Div.DocumentPath
      , Div.CoordinatorName
      , Div.CoordinatorPassword
      , Div.TemplatePath
      , Div.ReportPath
      , Div.UseCurrencySpecialSymbol
      , Div.DebitTextAfterValue
      , Div.CreditTextAfterValue
      , Div.TemplateDocLibrary
      , Div.ReportDocLibrary
      , Div.InboxRegPath
      , Div.DatabaseName
      , Div.SpsDbSettingsEncrypted
      , Div.VersionDate
      , Div.IsUseSpecialAccount
      , Div.SpecialAccountUserName
      , Div.SpecialAccountPassword
      , Div.DocumentStorageSystem
    FROM
        dbo.todmDivisions AS Div
        CROSS APPLY
    (
        SELECT
            ServerName = REPLACE(Div.Server, 'http://', '')
          , SiteName   = REPLACE(Div.WorkSpace, 'sites/', '')
    )                     AS PthCalc )
   , ContractObjects
AS (SELECT
        ObjectID          = Facility.FacilityID
      , BusinessObject.ObjectCode
      , BusinessObject.ObjectName
      , Reference         = Facility.FacilityRef
      , Facility.UniqueMarketRef
      , Facility.InceptionDate
      , Facility.ExpiryDate
      , Facility.CreateDate
      , Department.DepartmentID
      , Department.DepartmentName
      , Facility.SubDepartmentID
      , SubDepartmentName = SubDepartment.Name
      , Facility.BrokerID
    FROM
        dbo.toFacility                   AS Facility
        INNER JOIN dbo.toFacilityTypes   AS FacilityType
            ON FacilityType.FacilityTypesID = Facility.TypeFacility
        INNER JOIN dbo.toBusinessObjects AS BusinessObject
            ON BusinessObject.ObjectCode = FacilityType.Code
        INNER JOIN dbo.toDepartments     AS Department
            ON Department.DepartmentID = Facility.DepartmentID
        INNER JOIN dbo.toSubDepartments  AS SubDepartment
            ON SubDepartment.SubDepartmentID = Facility.SubDepartmentID
    UNION ALL
    SELECT
        ObjectID          = MasterPolicy.PolicyID
      , BusinessObject.ObjectCode
      , BusinessObject.ObjectName
      , Reference         = MasterPolicy.PolicyRef
      , MasterPolicy.UniqueMarketRef
      , MasterPolicy.InceptionDate
      , MasterPolicy.ExpiryDate
      , MasterPolicy.CreateDate
      , Department.DepartmentID
      , Department.DepartmentName
      , MasterPolicy.SubDepartmentID
      , SubDepartmentName = SubDepartment.Name
      , MasterPolicy.BrokerProducerID
    FROM
        dbo.toMasterPolicy               AS MasterPolicy
        INNER JOIN dbo.toDepartments     AS Department
            ON Department.DepartmentID = MasterPolicy.DepartmentID
        INNER JOIN dbo.toBusinessObjects AS BusinessObject
            ON BusinessObject.ObjectCode = 'P'
        INNER JOIN dbo.toSubDepartments  AS SubDepartment
            ON SubDepartment.SubDepartmentID = MasterPolicy.SubDepartmentID)
   , DocumentPrioritised
AS (SELECT
        Doc.DocumentID
      , Fld.FolderType
      , Obj.UniqueMarketRef
      , Obj.Reference
      , Obj.InceptionDate
      , BusObj.ObjectName
      , Doc.RootObjectID
      , Departments.DepartmentName
      , Departments.DepartmentID
      , Obj.SubDepartmentName
      , Pth.ServerName
      , Pth.SiteName
      , Doc.FolderID
      , Doc.DocumentName
      , DocumentTypePriorityOrdinal = DocumentTypePriority.Ordinal
      , DocumentPriorityOrdinal     = ROW_NUMBER() OVER (PARTITION BY
                                                             Doc.RootObjectID
                                                           , Doc.RootObjectCode
                                                         ORDER BY DocumentTypePriority.Ordinal ASC
                                                                , Doc.CreateDate DESC
                                                        )
      , Document.DocumentFileType
      , Document.DocumentFilePath
      , Document.DocumentFileURL
      , Pth.LibraryPath
      , FolderCalc.FolderPath
      , [FileName]                  = STUFF(Doc.FileName, 1, 1, '')
      , Doc.SettingsVersion
      , Doc.TemplateID
      , Doc.DocNumber
      , Doc.CreateDate
      , Doc.UserName
    FROM
        dbo.todmDocuments                AS Doc
        INNER JOIN dbo.todmFolders       AS Fld
            ON Fld.FolderID = Doc.FolderID
        INNER JOIN DivisionPath          AS Pth
            ON Pth.DivisionID = Doc.DepartmentID
               AND ISNULL(Pth.Version, 0) = ISNULL(Doc.SettingsVersion, 0)
		INNER JOIN dbo.todmDocumentSystemStatuses AS DocSysSt
			ON DocSysSt.DocumentSystemStatusID = Doc.DocumentSystemStatusID
        INNER JOIN dbo.toBusinessObjects AS BusObj
            ON BusObj.ObjectCode = Doc.RootObjectCode
        INNER JOIN dbo.toDepartments     AS Departments
            ON Departments.DepartmentID = Doc.DepartmentID
        INNER JOIN ContractObjects       AS Obj
            ON Obj.ObjectID = Doc.RootObjectID
               AND Obj.ObjectCode = BusObj.ObjectCode
        CROSS APPLY
    (
        SELECT
            FolderPath = COALESCE(
                                     REPLACE(NULLIF(Fld.FolderPath, ''), '/', '\')
                                   , '\' + Fld.SectionKey + '\' + Fld.FolderType
                                 )
          , FolderURL  = COALESCE(NULLIF(Fld.FolderPath, ''), '/' + Fld.SectionKey + '/' + Fld.FolderType)
    )                                    AS FolderCalc
        CROSS APPLY
    (
        SELECT
            DocumentFilePath = CONCAT(
                                         Pth.LibraryPath
                                       , FolderCalc.FolderPath
                                       , '_' + CAST(NULLIF(Doc.FolderNumber, 0) AS VARCHAR(2))
                                       , REPLACE(Doc.FileName, '/', '\')
                                     )
          , DocumentFileURL  = CONCAT(
                                         Pth.LibraryURL
                                       , FolderCalc.FolderURL
                                       , '_' + CAST(NULLIF(Doc.FolderNumber, 0) AS VARCHAR(2))
                                       , Doc.FileName
                                     )
          , DocumentFileType = CASE
                                   WHEN Doc.FileName LIKE '%.%' THEN
                                       REVERSE(LEFT(REVERSE(Doc.FileName), CHARINDEX('.', REVERSE(Doc.FileName)) - 1))
                                   ELSE
                                       NULL
                               END
    ) AS Document
        /*
------------------------------------------------------------------------
Document Priority: select a document according to the prioroty listed
------------------------------------------------------------------------
Most recent Placing word doc that contains slip in the name
Most recent signed word doc that contains slip in the name
Most recent Placing pdf that contains slip in the name
Most recent signed pdf that contains slip in the name
Most recent Placing word doc
Most recent signed word doc
Most recent placing pdf
Most recent signed pdf
Most Recent Placing slip
Most Recent Signed Slip
Most Recent Quote Slip
*/
        -- 
        CROSS APPLY
    (
        SELECT
            Ordinal = CASE
           		  WHEN Fld.FolderTypeID in ( 60, 97 )-- Signed and signing slip
                               AND Doc.DocumentName LIKE '%Slip%'
							   OR  Doc.DocumentName LIKE '%MRC%'
							   OR  Doc.DocumentName LIKE '%Contract%'
							   OR  Doc.DocumentName LIKE '%London Market%'
							   OR  Doc.DocumentName LIKE '%Final%'
							   --OR Doc.DocumentName IN (select PolicyRef from dbo.toMasterPolicy)
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
							  1
						 --WHEN Fld.FolderTypeID in ( 60, 97 )-- Signed and signing slip
							--   AND  Doc.DocumentName IN (select PolicyRef from dbo.toMasterPolicy) THEN
							--  2
						 WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                               AND Doc.DocumentName LIKE '%Sign%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              3
					     WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                               AND Doc.DocumentName LIKE '%Scan%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              4
						 WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                               AND Doc.DocumentName LIKE '%FOS%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              5
						 WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                               AND Doc.DocumentName LIKE '%Policy%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              6
                          WHEN Fld.FolderTypeID = 44 -- Placing slip
                               AND Doc.DocumentName LIKE '%Slip%'
							   OR  Doc.DocumentName LIKE '%MRC%'
							   OR  Doc.DocumentName LIKE '%Contract%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              7
						  --WHEN Fld.FolderTypeID = 44 -- Placing slip
							 --  AND  Doc.DocumentName IN (select PolicyRef from dbo.toMasterPolicy) THEN
							 -- 8
                          WHEN Fld.FolderTypeID = 44 -- Placing slip
							   AND Doc.DocumentName LIKE '%Sign%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              9
						  WHEN Fld.FolderTypeID = 44 -- Placing slip
							   AND Doc.DocumentName LIKE '%Scan%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              10
						  WHEN Fld.FolderTypeID = 44 -- Placing slip
                               AND Doc.DocumentName LIKE '%FOS%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              11
						  WHEN Fld.FolderTypeID = 44 -- Placing Slip
                               AND Doc.DocumentName LIKE '%Policy%'
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              12
						  WHEN Fld.FolderTypeID IN ( 60, 97 ) -- Signed and signing slip
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              13
                          WHEN Fld.FolderTypeID = 44 -- Placing slip
                               AND Document.DocumentFileType IN ( 'pdf' ) THEN
                              14

                          ELSE
                              99
                      END
    ) AS DocumentTypePriority
    WHERE 1 = 1
          AND Fld.FolderTypeID IN ( 44, 52, 60, 83, 97 )
		  AND DocSysSt.StatusName = 'Exists' -- Only inlcude docments that currently exist
		  -- Date created
          AND Obj.InceptionDate > '2021-07-01'
		  AND Doc.CreateDate < '2022-02-15'
   )
SELECT
    DocPriority.DocumentID
  , DocPriority.FolderType
  , DocPriority.UniqueMarketRef
  , DocPriority.RootObjectID
  , DocPriority.Reference
  , DocPriority.InceptionDate
  , DocPriority.ObjectName
  , DocPriority.DepartmentID
  , DocPriority.DepartmentName
  , DocPriority.SubDepartmentName
  , DocPriority.ServerName
  , DocPriority.SiteName
  , DocPriority.FolderID
  , DocPriority.DocumentTypePriorityOrdinal
  , DocPriority.DocumentPriorityOrdinal
  , DocPriority.DocumentName
  , DocPriority.DocumentFileType
  , DocPriority.DocumentFilePath
  , DocPriority.DocumentFileURL
  , DocPriority.LibraryPath
  , DocPriority.FolderPath
  , DocPriority.FileName
  , DocPriority.DocNumber
  , DocPriority.CreateDate
FROM
    DocumentPrioritised AS DocPriority
WHERE 1 = 1
      AND DocPriority.DocumentTypePriorityOrdinal  = 1
	  AND DocPriority.DocumentFileType = 'pdf'
	  --AND DocPriority.UniqueMarketRef in ( )
    and DocPriority.DocumentName 
		NOT LIKE '%Note%'
	and DocPriority.DocumentName 
		NOT LIKE '%Bind%'
	and DocPriority.DocumentName 
		NOT LIKE '%Binder%'
	and DocPriority.DocumentName 
		NOT LIKE '%Cert%'
	and DocPriority.DocumentName 
		NOT LIKE '%Certificate%'
	and DocPriority.DocumentName 
		NOT LIKE '%Invoices%'
	and DocPriority.DocumentName 
		NOT LIKE '%Withhold%'
	and DocPriority.DocumentName 
		NOT LIKE '%Wording%' 
	and DocPriority.DocumentName 
		NOT LIKE '%Debit%'
	and DocPriority.DocumentName 
		NOT LIKE '%DebitNote%'
	and DocPriority.DocumentName 
		NOT LIKE '%Credit%'
	and DocPriority.DocumentName 
		NOT LIKE '%Note%'
	and DocPriority.DocumentName 
		NOT LIKE '%CoverNote%'
	and DocPriority.DocumentName 
		NOT LIKE '%Quote%'
	and DocPriority.DocumentName 
		NOT LIKE '%Client%'
	and DocPriority.DocumentName 
		NOT LIKE '%cc%'
	and DocPriority.DocumentName 
		NOT LIKE '%cv%'
	and DocPriority.DocumentName
		NOT LIKE '%subscription%'
		and DocPriority.DocumentName
		NOT LIKE '%FON%'
	AND DocPriority.DocumentName
		NOT LIKE '%Endt%'
	AND DocPriority.DocumentName
		NOT LIKE '%Endors%'
	AND DocPriority.DocumentName
		NOT LIKE '%Placing%'
	AND DocPriority.DocumentName
		NOT LIKE '%Amend%'
	AND DocPriority.DocumentName
		NOT LIKE '%Excess%'
	AND DocPriority.DocumentName
		NOT LIKE '%xs%'
	AND DocPriority.DocumentName
		NOT LIKE '%dec%'
	AND DocPriority.DocumentName
		NOT LIKE '%schedule%'
	AND DocPriority.DocumentName
		NOT LIKE '%cover%'
	AND DocPriority.DocumentName
		NOT LIKE '%consort%'
ORDER BY DocPriority.UniqueMarketRef DESC
       , DocPriority.DocumentPriorityOrdinal ASC;
""", RKH_DB23)

