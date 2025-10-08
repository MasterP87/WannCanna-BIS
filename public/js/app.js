// app.js – client side logic for the appointment system

(() => {
  /**
   * Utility to format ISO date/time strings into a more readable form.  If the
   * browser supports Intl.DateTimeFormat it will be used to respect the
   * client's locale; otherwise the raw string is returned.
   */
  function formatDateTime(isoString) {
    try {
      const date = new Date(isoString);
      return new Intl.DateTimeFormat(undefined, {
        dateStyle: 'medium',
        timeStyle: 'short'
      }).format(date);
    } catch (e) {
      return isoString;
    }
  }

  /**
   * Initialize seller dashboard.  Expects the page to define global
   * variables `appointments`, `bookings`, `total` and `user` from
   * server‑side rendering.  Renders appointment and booking tables and
   * listens for SSE events to update the dashboard in real time.
   */
  function initSellerDashboard() {
    const appointmentsTable = document.getElementById('appointmentsTable');
    const bookingsTable = document.getElementById('bookingsTable');
    const totalEl = document.getElementById('total');

    function render() {
      // Clear existing rows
      appointmentsTable.querySelector('tbody').innerHTML = '';
      bookingsTable.querySelector('tbody').innerHTML = '';
      // Render appointments
      appointments.forEach(app => {
        const tr = document.createElement('tr');
        const bookedStatus = app.booked ? 'Ja' : 'Nein';
        // Determine display string for date/time.  Support allDay and end times.
        let timeText;
        const startVal = app.start || app.datetime;
        const endVal = app.end;
        if (app.allDay) {
          try {
            const d = new Date(startVal);
            timeText = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium' }).format(d) + ' (ganztägig)';
          } catch (e) {
            timeText = startVal;
          }
        } else if (endVal) {
          timeText = formatDateTime(startVal) + ' – ' + formatDateTime(endVal);
        } else {
          timeText = formatDateTime(startVal);
        }
        // Build action column.  Sellers may edit or delete unbooked appointments.
        let actionHtml = '';
        if (!app.booked) {
          actionHtml += '<a href="/seller/appointments/edit/' + app.id + '">Bearbeiten</a> ';
          actionHtml += '<form method="post" action="/seller/appointments/delete/' + app.id + '" onsubmit="return confirm(\'Termin löschen?\');" style="display:inline; margin-left:0.5em;"><button type="submit">Löschen</button></form>';
        }
        tr.innerHTML = `
          <td>${timeText}</td>
          <td>${app.location}</td>
          <td>${bookedStatus}</td>
          <td>${actionHtml}</td>
        `;
        appointmentsTable.querySelector('tbody').appendChild(tr);
      });
      // Render bookings
      bookings.forEach(b => {
        const appointment = appointments.find(a => a.id === b.appointmentId);
        if (!appointment) return;
        // Determine time text similar to appointments view
        let timeText;
        const startVal = appointment.start || appointment.datetime;
        const endVal = appointment.end;
        if (appointment.allDay) {
          try {
            const d = new Date(startVal);
            timeText = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium' }).format(d) + ' (ganztägig)';
          } catch (e) {
            timeText = startVal;
          }
        } else if (endVal) {
          timeText = formatDateTime(startVal) + ' – ' + formatDateTime(endVal);
        } else {
          timeText = formatDateTime(startVal);
        }
        const tr = document.createElement('tr');
        tr.innerHTML = `
          <td>${timeText}</td>
          <td>${appointment.location}</td>
          <td>${b.buyerName || 'Unbekannt'}</td>
          <td>${b.amount.toFixed(2)} €</td>
          <td>
            <form method="post" action="/seller/bookings/cancel/${b.id}" onsubmit="return confirm('Buchung stornieren?');">
              <button type="submit">Stornieren</button>
            </form>
          </td>
        `;
        bookingsTable.querySelector('tbody').appendChild(tr);
      });
      totalEl.textContent = bookings.reduce((sum, b) => sum + b.amount, 0).toFixed(2) + ' €';
    }
    render();
    // SSE
    const es = new EventSource('/events');
    es.addEventListener('booking', event => {
      const msg = JSON.parse(event.data);
      if (msg.action === 'created') {
        // Add booking
        bookings.push(msg.booking);
        // Update appointment
        const app = appointments.find(a => a.id === msg.booking.appointmentId);
        if (app) {
          app.booked = true;
          app.bookingId = msg.booking.id;
        }
      } else if (msg.action === 'updated') {
        const idx = bookings.findIndex(b => b.id === msg.booking.id);
        if (idx >= 0) bookings[idx] = msg.booking;
      } else if (msg.action === 'cancelled') {
        const idx = bookings.findIndex(b => b.id === msg.bookingId);
        if (idx >= 0) bookings.splice(idx, 1);
        // Find appointment and mark available
        const app = appointments.find(a => a.bookingId === msg.bookingId);
        if (app) {
          app.booked = false;
          app.bookingId = null;
        }
      }
      render();
    });
    es.addEventListener('appointment', event => {
      const msg = JSON.parse(event.data);
      if (msg.action === 'created') {
        appointments.push(msg.appointment);
        render();
      } else if (msg.action === 'updated') {
        const idx = appointments.findIndex(a => a.id === msg.appointment.id);
        if (idx >= 0) {
          appointments[idx] = msg.appointment;
        }
        render();
      } else if (msg.action === 'deleted') {
        const idx = appointments.findIndex(a => a.id === msg.appointmentId);
        if (idx >= 0) {
          appointments.splice(idx, 1);
        }
        render();
      }
    });
  }

  /**
   * Initialize page for viewing a seller's appointments as a buyer.  Uses
   * global `seller` and `appointments` variables injected by the server.
   */
  function initBuyerViewSeller() {
    const listEl = document.getElementById('appointmentList');
    // Flag indicating whether the buyer has been approved by the seller
    const approvedFlag = (typeof approved === 'boolean' ? approved : (approved === 'true'));
    // Render the list of appointments.  For each available appointment
    // render product selection, amount input and booking button when the
    // buyer is approved.  Otherwise display a note about missing approval.
    function render() {
      listEl.innerHTML = '';
      appointments.forEach(app => {
        const li = document.createElement('li');
        const isBooked = app.booked;
        // Compute time display for appointment
        let timeText;
        const startVal = app.start || app.datetime;
        const endVal = app.end;
        if (app.allDay) {
          try {
            const d = new Date(startVal);
            timeText = new Intl.DateTimeFormat(undefined, { dateStyle: 'medium' }).format(d) + ' (ganztägig)';
          } catch (e) {
            timeText = startVal;
          }
        } else if (endVal) {
          timeText = formatDateTime(startVal) + ' – ' + formatDateTime(endVal);
        } else {
          timeText = formatDateTime(startVal);
        }
        let html = `<span>${timeText} - ${app.location} ${isBooked ? '(gebucht)' : ''}</span>`;
        if (!isBooked) {
          if (approvedFlag) {
            // Build select options for products including current stock
            let options = '';
            products.forEach(p => {
              const stockText = (p.stock !== undefined ? ` (Verfügbar: ${p.stock})` : '');
              options += `<option value="${p.id}">${p.name}${stockText}</option>`;
            });
            html += `\n<select class="productSelect">${options}</select>\n`;
            html += `<input type="number" class="amountInput" min="5" max="500" step="5" placeholder="Betrag">`;
            html += ` <span class="qty"></span> `;
            html += `<button type="button" class="bookBtn" data-id="${app.id}">Buchen</button>`;
          } else {
            html += ` <span class="not-approved">Freischaltung erforderlich</span>`;
          }
        }
        li.innerHTML = html;
        listEl.appendChild(li);
      });
    }
    render();
    // Update displayed quantity when amount or product changes
    listEl.addEventListener('input', function(e) {
      if (e.target.classList.contains('amountInput') || e.target.classList.contains('productSelect')) {
        const li = e.target.closest('li');
        const amountInput = li.querySelector('.amountInput');
        const productSelect = li.querySelector('.productSelect');
        const qtySpan = li.querySelector('.qty');
        const amountVal = parseInt(amountInput.value, 10);
        const productId = productSelect ? productSelect.value : null;
        if (!amountVal || amountVal < 5 || amountVal > 500 || amountVal % 5 !== 0 || !productId) {
          qtySpan.textContent = '';
          return;
        }
        const ts = tiers[productId];
        const price = resolveUnitPrice(ts, amountVal);
        if (!price) {
          qtySpan.textContent = '';
          return;
        }
        const q = Math.floor((amountVal / price) * 100) / 100;
        const product = products.find(p => p.id === productId);
        let msg = `→ ${q} Stk.`;
        if (product && typeof product.stock === 'number') {
          if (q > product.stock) {
            msg += ` (max ${product.stock} verfügbar)`;
          }
        }
        qtySpan.textContent = msg;
      }
    });
    // Handle booking click
    listEl.addEventListener('click', function(e) {
      if (e.target.classList.contains('bookBtn')) {
        const li = e.target.closest('li');
        const appointmentId = e.target.getAttribute('data-id');
        const productSelect = li.querySelector('.productSelect');
        const amountInput = li.querySelector('.amountInput');
        const productId = productSelect ? productSelect.value : null;
        const amountVal = parseInt(amountInput.value, 10);
        if (!productId) {
          alert('Bitte Produkt wählen');
          return;
        }
        if (!amountVal || amountVal < 5 || amountVal > 500 || amountVal % 5 !== 0) {
          alert('Ungültiger Betrag (5-500 in 5er Schritten)');
          return;
        }
        const ts = tiers[productId];
        const price = resolveUnitPrice(ts, amountVal);
        if (!price) {
          alert('Keine gültige Staffel für diesen Betrag');
          return;
        }
        const q = Math.floor((amountVal / price) * 100) / 100;
        const product = products.find(p => p.id === productId);
        if (product && typeof product.stock === 'number' && q > product.stock) {
          alert('Der Verkäufer hat nur ' + product.stock + ' Stück verfügbar. Bitte Menge verringern.');
          return;
        }
        if (!confirm(`Du erhältst ${q} Stück. Buchung ausführen?`)) {
          return;
        }
        // Submit booking via form
        const form = document.createElement('form');
        form.method = 'post';
        form.action = '/buyer/book';
        form.innerHTML = '<input type="hidden" name="appointmentId" value="' + appointmentId + '">' +
          '<input type="hidden" name="amount" value="' + amountVal + '">' +
          '<input type="hidden" name="productId" value="' + productId + '">';
        document.body.appendChild(form);
        form.submit();
      }
    });
    // SSE to update list when another buyer books or cancels
    const es = new EventSource('/events');
    es.addEventListener('booking', event => {
      const msg = JSON.parse(event.data);
      if (msg.action === 'created') {
        const app = appointments.find(a => a.id === msg.booking.appointmentId);
        if (app) {
          app.booked = true;
          app.bookingId = msg.booking.id;
        }
      } else if (msg.action === 'cancelled') {
        const app = appointments.find(a => a.bookingId === msg.bookingId);
        if (app) {
          app.booked = false;
          app.bookingId = null;
        }
      }
      render();
    });

    // Listen for stock update events.  When a stock event is received,
    // update the local products array and re-render the appointment list
    // so that stock information and quantity checks are correct.
    es.addEventListener('stock', event => {
      try {
        const msg = JSON.parse(event.data);
        const product = products.find(p => p.id === msg.productId);
        if (product) {
          product.stock = msg.stock;
          render();
        }
      } catch (e) {
        console.error('Fehler beim Verarbeiten des Stock-Events', e);
      }
    });
  }

  /**
   * Initialize the buyer bookings page.  Expects global `bookings`,
   * `appointments` and `sellers`.  Renders the bookings table and listens
   * for real‑time updates.
   */
  function initBuyerBookings() {
    const table = document.getElementById('buyerBookingsTable');
    function render() {
      table.querySelector('tbody').innerHTML = '';
      bookings.forEach(b => {
        const appointment = appointments.find(a => a.id === b.appointmentId);
        const seller = sellers.find(s => s.id === appointment.sellerId);
        const tr = document.createElement('tr');
        // Compute display quantity: use stored quantity if available; fallback to empty string
        const qty = typeof b.quantity === 'number' ? (b.quantity.toFixed(2) + ' Stück') : '-';
        tr.innerHTML = `
          <td>${seller.name}</td>
          <td>${formatDateTime(appointment.datetime)}</td>
          <td>${appointment.location}</td>
          <td>${b.amount.toFixed(2)} €</td>
          <td>${qty}</td>
          <td>
            <a href="/buyer/bookings/edit/${b.id}">Bearbeiten</a>
          </td>
          <td>
            <form method="post" action="/buyer/bookings/cancel/${b.id}" onsubmit="return confirm('Buchung stornieren?');">
              <button type="submit">Stornieren</button>
            </form>
          </td>
        `;
        table.querySelector('tbody').appendChild(tr);
      });
    }
    render();
    // SSE to update bookings when changed by seller cancellation
    const es = new EventSource('/events');
    es.addEventListener('booking', event => {
      const msg = JSON.parse(event.data);
      if (msg.action === 'updated') {
        const idx = bookings.findIndex(b => b.id === msg.booking.id);
        if (idx >= 0) bookings[idx] = msg.booking;
      } else if (msg.action === 'cancelled') {
        const idx = bookings.findIndex(b => b.id === msg.bookingId);
        if (idx >= 0) bookings.splice(idx, 1);
      } else if (msg.action === 'created') {
        bookings.push(msg.booking);
      }
      render();
    });
  }

  // Expose initialisers to the global scope so templates can call them
  window.AppointmentApp = {
    initSellerDashboard,
    initBuyerViewSeller,
    initBuyerBookings
  };
})();
