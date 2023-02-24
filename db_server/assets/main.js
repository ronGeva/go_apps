let socket = new WebSocket("ws://127.0.0.1:5678/ws");
console.log("Attempting Connection...");

socket.onopen = () => {
    console.log("Successfully Connected");
};

socket.onclose = event => {
    console.log("Socket Closed Connection: ", event);
};

socket.onerror = error => {
    console.log("Socket Error: ", error);
};

function SendQuery() {
    let dbName = document.getElementById("db").value;
    let query = document.getElementById("query").value;
    let message = {"type": "query", "db": dbName, "query": query};
    socket.send(JSON.stringify(message))
}

function CreateTable() {
  let dbPath = document.getElementById("dbPath").value;
  let message = {"type": "create", "db": dbPath};
  socket.send(JSON.stringify(message));
}

function PrintResultsTable(data) {
    let table = document.getElementById("results_table")
    table.innerHTML = ""
    for (let i = 0; i < data.length; i++) {
        let row = table.insertRow(-1)
        for(let j = 0; j < data[i].length; j++) {
            let cell = row.insertCell(-1)
            cell.innerHTML = data[i][j]
        }
    }
}

function HandleMessage(msg) {
    let res = JSON.parse(msg.data);
    if ("Success" in res) {
      if (!res["Success"]) {
        console.log(res["Error"])
      } else {
        console.log(res["Data"])
        PrintResultsTable(res["Data"])
      }
    }
}

socket.onmessage = msg => {
    HandleMessage(msg)
}
