/*global tableau */

// source https://glitch.com/edit/#!/simple-airtable-wdc?path=README.md:1:0
let myConnector = tableau.makeConnector();

myConnector.getSchema = async schemaCallback => {
  console.log("Creating Schema!")
  const connectionData = JSON.parse(tableau.connectionData);
  let tables = connectionData.tables.split(",");
  tables = tables.map(table => table.trim()).filter(table => table !== "");
  let url = `https://api.airtable.com/v0/${connectionData.base}`;
  let options = {
    headers: {
      Authorization: "Bearer " + tableau.password
    }
  };
  let tableData = [];
  let tableSchemas = [];

  for (let table of tables) {
    let records = [];
    let offset = "";
    let morePages = true;
    do {
      try {
        const tableURL = `${url}/${table}${offset ? "?offset=" + offset : ""}`;
        const response = await fetch(tableURL, options);
        const data = await response.json();
        if (data.error) {
          morePages = false;
          tableau.abortWithError(data.error);
        }
        if (!data.offset) {
          morePages = false;
        } else {
          offset = data.offset;
        }
        for (let record of data.records) {
          records.push({
            ...record.fields,
            airtable_record_id: record.id,
            createdTime: record.createdTime
          });
        }
      } catch (err) {
        morePages = false;
        tableau.abortWithError(err);
      }
    } while (morePages);

    tableData[table] = records;
  }

  for (let table of tables) {
    const dataTypes = {
      number: tableau.dataTypeEnum.float,
      boolean: tableau.dataTypeEnum.bool,
      string: tableau.dataTypeEnum.string,
      object: tableau.dataTypeEnum.string
    };
    let columns = [];

    for (let record of tableData[table]) {
      for (let column in record) {
        if (
          !columns.find(function(f) {
            return f.alias === column;
          })
        ) {

          let safeToAdd = false;
          let id = column.replace(/[^A-Za-z0-9_-]/gi, "");

          do {
            if(columns.find(c => c.id === id)) {
              id += "_copy";
            } else {
              safeToAdd = true;
            }
          } while (!safeToAdd)

          columns.push({
            id,
            alias: column,
            dataType: dataTypes[typeof record[column]]
          });
        }
      }
    }


    tableSchemas.push({
      id: table.replace(/[^A-Za-z0-9_-]/gi, ""),
      alias: table,
      columns: columns
    });
  }
  console.log(tableSchemas)

  schemaCallback(tableSchemas);
};

myConnector.getData = async (table, doneCallback) => {
  console.log("Getting data!")
  const connectionData = JSON.parse(tableau.connectionData);
  let tableName = table.tableInfo.alias;
  let url = `https://api.airtable.com/v0/${connectionData.base}/${tableName}`;
  let options = {
    headers: {
      Authorization: "Bearer " + tableau.password
    }
  };
  let records = [];
  let offset = "";
  let morePages = true;

  do {
    try {
      const tableURL = `${url}${offset ? "?offset=" + offset : ""}`;
      console.log(tableURL)
      const response = await fetch(tableURL, options);
      const data = await response.json();
      console.log(data)
      if (data.error) {
        morePages = false;
        tableau.abortWithError(data.error);
      }
      if (!data.offset) {
        morePages = false;
      } else {
        offset = data.offset;
      }
      for (let record of data.records) {
        records.push({
          ...record.fields,
          airtable_record_id: record.id,
          createdTime: record.createdTime
        });
      }
    } catch (err) {
      morePages = false;
      tableau.abortWithError(err);
    }
  } while (morePages);

  let columns = [];
  for (let record of records) {
    for (let column in record) {
      if (
        !columns.find(function(f) {
          return f.name === column;
        })
      ) {
        columns.push({
          id: column.replace(/[^A-Z,a-z]/gi, ""),
          alias: column,
          dataType: typeof record[column]
        });
      }
    }
  }

  let tableData = [];

  for (let record of records) {
    let row = {
      "airtable_record_id": record.airtable_record_id
    };
    for (let column of columns) {
      switch (column.dataType) {
        case "boolean":
          row[column.id] = record[column.alias] || false;
          break;
        case "object":
          row[column.id] = record[column.alias]
            ? record[column.alias].toString()
            : "";
          break;
        default:
          row[column.id] = record[column.alias] || "";
          break;
      }
    }
    tableData.push(row);
  }

  console.log(tableData)

  table.appendRows(tableData);
  doneCallback();
};

tableau.registerConnector(myConnector);

function submit() {
  tableau.connectionName = "AirTable";
  const data = {
    base: $("#base").val(),
    tables: $("#tables").val()
  };
  tableau.connectionData = JSON.stringify(data);
  tableau.password = $("#key").val();
  tableau.submit();
}
