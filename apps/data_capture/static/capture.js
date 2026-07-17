(() => {
  "use strict";
  const form = document.querySelector(".capture-form");
  if (!form) return;
  const number = value => Number.parseFloat(value || "0") || 0;
  const money = value => `K${value.toFixed(2)}`;

  const renumber = list => {
    list.querySelectorAll(".repeat-row").forEach((row, index) => {
      const label = row.querySelector(".row-number");
      if (label) label.textContent = String(index + 1);
      const remove = row.querySelector(".remove-row");
      if (remove) remove.hidden = list.children.length === 1;
    });
  };
  document.querySelectorAll(".add-row").forEach(button => button.addEventListener("click", () => {
    const list = document.querySelector(`[data-list="${button.dataset.target}"]`);
    const clone = list.firstElementChild.cloneNode(true);
    clone.querySelectorAll("input,select,textarea").forEach(input => {
      if (input.tagName === "SELECT") input.selectedIndex = 0;
      else input.value = "";
    });
    list.appendChild(clone);
    renumber(list);
    calculate();
  }));
  document.addEventListener("click", event => {
    if (!event.target.classList.contains("remove-row")) return;
    const row = event.target.closest(".repeat-row");
    const list = row.parentElement;
    if (list.children.length > 1) row.remove();
    renumber(list);
    calculate();
  });

  const setText = (selector, value) => {
    const element = document.querySelector(selector);
    if (element) element.textContent = value;
  };
  const calculate = () => {
    if (form.dataset.formType === "sales") {
      let cash = 0, card = 0, variance = 0;
      document.querySelectorAll(".till-row").forEach(row => {
        const rowCash = number(row.querySelector(".cash").value);
        const rowCard = number(row.querySelector(".card").value);
        const rowVariance = rowCash + rowCard - number(row.querySelector(".z-reading").value);
        row.querySelector(".variance").textContent = money(rowVariance);
        row.classList.toggle("unbalanced", Math.abs(rowVariance) > 5);
        cash += rowCash; card += rowCard; variance += rowVariance;
      });
      const sales = cash + card;
      setText('[data-total="cash"]', money(cash)); setText('[data-total="card"]', money(card));
      setText('[data-total="sales"]', money(sales)); setText('[data-total="variance"]', money(variance));
      setText("[data-control-variance]", money(variance));
      const door = number(form.elements.main_door.value), served = number(form.elements.served.value), hours = number(form.elements.labour_hours.value);
      setText('[data-derived="conversion"]', door ? `${(served / door * 100).toFixed(2)}%` : "—");
      setText('[data-derived="per-customer"]', served ? money(sales / served) : "—");
      setText('[data-derived="per-hour"]', hours ? money(sales / hours) : "—");
    } else if (form.dataset.formType === "staff-performance") {
      let moved = 0, assisting = 0;
      document.querySelectorAll(".staff-row").forEach(row => { moved += number(row.querySelector(".items-moved").value); assisting += number(row.querySelector(".assisting").value); });
      setText('[data-total="moved"]', String(moved)); setText('[data-total="assisting"]', String(assisting));
    } else if (form.dataset.formType === "attendance") {
      const counts = {present:0,off:0,leave:0,sick:0,absent:0};
      document.querySelectorAll('[name="attendance_status"]').forEach(select => { if (counts[select.value] !== undefined) counts[select.value]++; });
      Object.entries(counts).forEach(([key,value]) => setText(`[data-status="${key}"]`, String(value)));
    } else if (form.dataset.formType === "bales") {
      let weight = 0, qty = 0, amount = 0;
      const rows = document.querySelectorAll(".bale-row");
      rows.forEach(row => {
        const rowQty = number(row.querySelector(".qty").value), rowAmount = number(row.querySelector(".amount").value);
        row.querySelector(".unit-price").textContent = rowQty ? money(rowAmount / rowQty) : "—";
        weight += number(row.querySelector(".weight").value); qty += rowQty; amount += rowAmount;
      });
      setText('[data-total="bales"]', String(rows.length)); setText('[data-total="weight"]', `${weight.toFixed(3)} kg`);
      setText('[data-total="qty"]', String(qty)); setText('[data-total="amount"]', money(amount));
    }
  };
  form.addEventListener("input", calculate);
  form.addEventListener("change", calculate);
  document.querySelectorAll(".repeat-list").forEach(renumber);
  const dateInput = form.elements.report_date;
  const checkDate = () => {
    const warning = document.querySelector(".date-warning");
    if (!warning || !dateInput.value) return;
    const age = (Date.now() - new Date(`${dateInput.value}T00:00:00`).getTime()) / 86400000;
    warning.hidden = age <= 3;
  };
  dateInput?.addEventListener("change", checkDate);
  checkDate(); calculate();
})();
