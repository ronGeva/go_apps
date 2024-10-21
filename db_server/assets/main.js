function logError(err) {
    window.alert(err)
}

let socket = null

function SendQuery() {
    let dbName = document.getElementById("db").value;
    let query = document.getElementById("query").value;
    let username = document.getElementById("username").value;
    let password = document.getElementById("password").value;
    let source_ip = document.getElementById("source_ip").value;
    let multi_prov_aggregation = document.getElementById("multi_prov_aggregation").value;
    let message = {"type": "query", "db": dbName, "query": query, "username": username,
         "password": password, "source_ip": source_ip, "aggregation": multi_prov_aggregation};
    socket.send(JSON.stringify(message))
}

function CreateTable() {
  let dbPath = document.getElementById("dbPath").value;
  let message = {"type": "create", "db": dbPath};
  socket.send(JSON.stringify(message));
}

function QueryExistingDBs() {
    let message = {"type": "queryDBs"};
    socket.send(JSON.stringify(message))
}

function PrintResultsTable(headers, data) {
    let table = document.getElementById("results_table")
    table.innerHTML = ""
    let header_row = table.insertRow(-1)
    header_row.style.fontWeight = "bold";
    for(let j = 0; j < headers.length; j++) {
        let cell = header_row.insertCell(-1)
        cell.innerHTML = headers[j]
    }

    for (let i = 0; i < data.length; i++) {
        let row = table.insertRow(-1)
        for(let j = 0; j < data[i].length; j++) {
            let cell = row.insertCell(-1)
            cell.innerHTML = data[i][j]
        }
    }
}

function ShowDBs(DBList) {
    dbDataList = document.getElementById("db_options")
    // clear existing list
    dbDataList.innerHTML = ""

    for (let index = 0; index < DBList.length; index++) {
        let option = document.createElement("option")
        option.value = DBList[index][0]
        dbDataList.appendChild(option)
    }
}

function HandleSuccessfulMessage(res) {
    if (res["Type"] == "query") {
        PrintResultsTable(res["Headers"], res["Data"])
        return
    }
    if (res["Type"] == "DBs") {
        ShowDBs(res["Data"])
        return
    }
    if (res["Type"] == "DBCreation") {
        // Do nothing
        return
    }
    logError("faulty message: " + res)
}

function HandleMessage(msg) {
    let res = JSON.parse(msg.data);
    if ("Success" in res) {
      if (!res["Success"]) {
        logError(res["Error"])
      } else {
        HandleSuccessfulMessage(res)
      }
    }
}

function setConnectionStatus(connection_status) {
    connection_status_obj = document.getElementById("connection_status")
    connection_status_obj.innerHTML = connection_status
}

function Connect() {
    ip = document.getElementById("server_ip").value;
    socket = new WebSocket("ws://" + ip + ":5678/ws");
    console.log("attempting connection to " + ip)
    setConnectionStatus("")

    socket.onopen = () => {
        console.log("Successfully Connected");
        setConnectionStatus("connected to " + ip)
    };

    socket.onclose = event => {
        console.log("Socket Closed Connection: ", event);
    };

    socket.onerror = error => {
        logError("Socket Error: " + error)
    };

    socket.onmessage = msg => {
        HandleMessage(msg)
    }
}

Connect()
