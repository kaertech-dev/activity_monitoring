// Handle dropdown item clicks to show corresponding form
document.querySelectorAll('.dropdown-item').forEach(item => {
    item.addEventListener('click', function (e) {
        e.preventDefault();
        setTimeout(() => {
            document.getElementById('form-container').style.display = 'block';
            document.getElementById('spinner').style.display = 'none';

            // Hide all forms
            document.querySelectorAll('#form-container form').forEach(form => {
                form.style.display = 'none';
            });

            // Show selected form
            const target = this.getAttribute('data-target');
            const selectedForm = document.querySelector(target);
            if (selectedForm) selectedForm.style.display = 'block';
        }, 500);
    });
});

// Auto-hide forms when dropdown closes
const dropdown = document.querySelector('.dropdown');
dropdown.addEventListener('hidden.bs.dropdown', function () {
    document.getElementById('form-container').style.display = 'none';
    document.querySelectorAll('#form-container form').forEach(form => {
        form.style.display = 'none';
    });
});

// Handle week form
document.getElementById('weekform')?.addEventListener('submit', function(e) {
    e.preventDefault();
    const weekInput = document.getElementById('week').value;
    
    if (weekInput) {
        const [year, week] = weekInput.split("-W");
        const firstDayOfYear = new Date(year, 0, 1);
        const firstWeekDay = firstDayOfYear.getDay();
        const weekStartOffset = (firstWeekDay <= 4 ? firstWeekDay - 1 : firstWeekDay - 8);
        const weekStart = new Date(firstDayOfYear.getTime() + ((week - 1) * 7 + (1 - weekStartOffset)) * 86400000);

        const startDate = weekStart.toISOString().split('T')[0];
        const endDate = new Date(weekStart);
        endDate.setDate(endDate.getDate() + 6);
        const endDateStr = endDate.toISOString().split('T')[0];

        window.location.href = `/?start_date=${startDate}&end_date=${endDateStr}`;
    }
});

// Handle month form
document.getElementById('monthform')?.addEventListener('submit', function(e){
    e.preventDefault();
    const monthInput = document.getElementById('month').value;
    if (monthInput){
        const [year, month] = monthInput.split("-");
        const start_date = `${year}-${month}-01`;
        const endDateObj = new Date(year, month, 0);
        const endDate = endDateObj.toISOString().split('T')[0];
        window.location.href = `/?start_date=${start_date}&end_date=${endDate}`;
    }
});

// Handle day form
document.getElementById('dayform')?.addEventListener('submit', function(e){
    e.preventDefault();
    const dayInput = document.getElementById('day').value;
    if (dayInput){
        const start_date = dayInput;
        const endDate = new Date(start_date);
        endDate.setDate(endDate.getDate() + 1);
        const endDateStr = endDate.toISOString().split('T')[0];
        window.location.href = `/?start_date=${start_date}&end_date=${endDateStr}`;
    }
});

// Handle range form submission
document.getElementById('rangeform')?.addEventListener('submit', function(e) {
    e.preventDefault();
    const start = document.querySelector('.action-start')?.value;
    const end = document.querySelector('.action-end')?.value;
    if (start && end) {
        window.location.href = `/?start_date=${start}&end_date=${end}`;
    }
});

// Navigate to operator activity page
function operatorAction(operatorName) {
    if (operatorName) {
        window.location.href = "/operator/" + operatorName;
    }
}
window.operatorAction = operatorAction; // Make available in global scope

document.getElementById('customer').addEventListener('change', function () {
    const customer = this.value;
    const modelSelect = document.getElementById('model');
    const stationSelect = document.getElementById('station');

    // Reset dropdowns
    modelSelect.innerHTML = '<option>Loading...</option>';
    stationSelect.innerHTML = '<option>Loading...</option>';

    modelSelect.disabled = true;
    stationSelect.disabled = true;

    if (!customer) {
        modelSelect.innerHTML = '<option value="">All Models</option>';
        stationSelect.innerHTML = '<option value="">All Stations</option>';
        modelSelect.disabled = true;
        stationSelect.disabled = true;
        return;
    }

    fetch(`/api/get-models-stations?customer=${encodeURIComponent(customer)}`)
        .then(response => response.json())
        .then(data => {
            modelSelect.innerHTML = '<option value="">All Models</option>';
            stationSelect.innerHTML = '<option value="">All Stations</option>';

            data.models.forEach(model => {
                const option = document.createElement('option');
                option.value = model;
                option.textContent = model;
                modelSelect.appendChild(option);
            });

            data.stations.forEach(station => {
                const option = document.createElement('option');
                option.value = station;
                option.textContent = station;
                stationSelect.appendChild(option);
            });

            modelSelect.disabled = false;
            stationSelect.disabled = false;
        })
        .catch(error => {
            console.error('Error fetching models and stations:', error);
            modelSelect.innerHTML = '<option value="">All Models</option>';
            stationSelect.innerHTML = '<option value="">All Stations</option>';
            modelSelect.disabled = true;
            stationSelect.disabled = true;
        });
});
