document.addEventListener("DOMContentLoaded", async () => {
  // подключаем меню
  fetch("menu.html")
    .then(res => res.text())
    .then(html => document.getElementById("menu").innerHTML = html)
    .catch(() => console.warn("Меню не найдено"));

  try {
    const res = await fetch("http://192.168.1.47:8000/dbinfo"); 
    if (!res.ok) throw new Error("Ошибка API");

    const data = await res.json();

    // Если соединения нет
    if (!data || !data.dbname) {
      document.getElementById("db-summary").innerHTML = "<p>Нет подключения к БД</p>";
      return;
    }

    // Общая информация
    document.getElementById("db-summary").innerHTML = `
      <p><b>Имя БД:</b> ${data.dbname}</p>
      <p><b>Размер:</b> ${data.dbsize_bytes} байт</p>
      <p><b>Количество таблиц:</b> ${data.table_count}</p>
      <p><b>Пользователей:</b> ${data.user_count}</p>`
    ;

    // Таблицы
    const tbody = document.querySelector("#tables-info tbody");
    tbody.innerHTML = ""; // очищаем перед вставкой

    data.tables.forEach(tbl => {
      const row = document.createElement("tr");
      row.innerHTML = `
        <td>${tbl.name}</td>
        <td>${tbl.size_bytes}</td>
        <td>${tbl.indexes && tbl.indexes.length ? tbl.indexes.join(", ") : "-"}</td>
        <td>
          Автовакуум: ${tbl.last_update.last_autovacuum ?? "-"}<br>
          Автоанализ: ${tbl.last_update.last_autoanalyze ?? "-"}
        </td>`
      ;
      tbody.appendChild(row);
    });

  } catch (err) {
    document.getElementById("db-summary").innerHTML = `<p>Ошибка: ${err.message}</p>`;
  }
});