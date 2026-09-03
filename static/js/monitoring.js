// Function to initialize dropdown states on page load
function initializeDropdownStates() {
    const customer = document.getElementById('customer');
    const model = document.getElementById('model');
    const station = document.getElementById('station');

    // Get current values (might be pre-selected from URL)
    const customerValue = customer.value;
    const modelValue = model.value;

    console.log('Initializing dropdown states:', { customerValue, modelValue });

    if (!customerValue) {
        // No customer selected - disable model and station, set default options
        model.innerHTML = '<option value="">Select customer first</option>';
        station.innerHTML = '<option value="">Select customer first</option>';
        model.disabled = true;
        station.disabled = true;

        console.log('No customer selected - disabled model and station dropdowns');
    } else {
        // Customer is selected - enable model, but keep station disabled until model is selected
        model.disabled = false;
        station.disabled = true;
        station.innerHTML = '<option value="">Select model first</option>';

        console.log('Customer selected - enabled model dropdown, disabled station dropdown');

        // Trigger customer change to load appropriate models
        setTimeout(() => {
            customer.dispatchEvent(new Event('change'));

            // If model is also pre-selected, trigger model change after models are loaded
            if (modelValue) {
                setTimeout(() => {
                    const modelOption = model.querySelector(`option[value="${modelValue}"]`);
                    if (modelOption) {
                        model.value = modelValue;
                        model.dispatchEvent(new Event('change'));
                        console.log('Re-selected pre-selected model:', modelValue);
                    }
                }, 1000);
            }
        }, 100);
    }
}

// Loading state management
function setLoadingState(isLoading) {
    const customer = document.getElementById('customer');
    const model = document.getElementById('model');
    const station = document.getElementById('station');
    // FIXED: was 'filterSubmit', but the actual button id is 'filterSubmitBtn'
    // (mismatch used to throw "Cannot set properties of null" here, which
    // silently killed setLoadingState and, in operatorAction(), the
    // navigation line right after it).
    const submitBtn = document.getElementById('filterSubmitBtn');
    const arrowDropdown = document.getElementById('arrowDropdown');
    const submitBtns = document.querySelectorAll('#form-container button');
    const spinner = document.getElementById('spinner');

    // Disable/enable arrow dropdown button
    if (arrowDropdown) {
        arrowDropdown.disabled = isLoading;
        arrowDropdown.style.opacity = isLoading ? '0.6' : '1';
        arrowDropdown.style.cursor = isLoading ? 'not-allowed' : 'pointer';
    }

    // Disable/enable all form submit buttons
    submitBtns.forEach(btn => {
        btn.disabled = isLoading;
        btn.textContent = isLoading ? 'Loading...' : 'Submit';
    });

    // Show/hide global spinner (guarded - template doesn't currently have #spinner)
    if (spinner) {
        spinner.style.display = isLoading ? 'inline-block' : 'none';
    }

    // Disable/enable dropdowns and submit button
    customer.disabled = isLoading;
    model.disabled = isLoading;
    station.disabled = isLoading;
    if (submitBtn) {
        submitBtn.disabled = isLoading;
        submitBtn.textContent = isLoading ? 'Loading...' : 'Submit';
    }

    // Add visual feedback
    const dropdowns = [customer, model, station];
    dropdowns.forEach(dropdown => {
        if (isLoading) {
            dropdown.style.opacity = '0.6';
            dropdown.style.cursor = 'not-allowed';
        } else {
            dropdown.style.opacity = '1';
            dropdown.style.cursor = 'pointer';
        }
    });
}

// NOTE: The Day/Week/Month/Range arrow-dropdown (#arrowDropdown,
// .dropdown-menu.dropdown-menu-end) and its #form-container popup are fully
// owned by the inline <script> in monitoring_v1.html now - it handles
// showing/hiding the right form and closing the dropdown. This file used to
// ALSO attach a `.dropdown-item` click handler and a `hidden.bs.dropdown`
// listener that hid #form-container the instant the dropdown closed - which
// is exactly what the inline script does right after showing the form, so
// the two together caused the form to flash and immediately disappear.
// Those duplicate handlers (and the duplicate per-form submit handlers
// below, which duplicated the inline script's unified formConfigs logic)
// have been removed to eliminate that conflict. Do not re-add dropdown/
// form-container wiring here - it belongs in monitoring_v1.html only.

// Navigate to operator activity page
function operatorAction(operatorName) {
    if (operatorName) {
        setLoadingState(true);
        window.location.href = "/operator/" + encodeURIComponent(operatorName);
    }
}
window.operatorAction = operatorAction;

// Helper function to get current date filters from URL
function getCurrentDateFilters() {
    const urlParams = new URLSearchParams(window.location.search);
    return {
        start_date: urlParams.get('start_date'),
        end_date: urlParams.get('end_date')
    };
}

function getAppOrigin() {
    return window.location.port === ''
        ? `${window.location.protocol}//${window.location.hostname}:5000`
        : '';
}

// Helper function to build API URL with date filters
function buildApiUrl(baseUrl, customer = null, model = null, station = null) {
    const params = new URLSearchParams();

    if (customer) params.append('customer', customer);
    if (model) params.append('model', model);
    if (station) params.append('station', station);

    // Add current date filters
    const dateFilters = getCurrentDateFilters();
    if (dateFilters.start_date) params.append('start_date', dateFilters.start_date);
    if (dateFilters.end_date) params.append('end_date', dateFilters.end_date);

    // The dashboard is currently served from port 80 while Flask listens on 5000.
    // Keep same-origin requests unchanged when an explicit port or proxy is used.
    return `${getAppOrigin()}${baseUrl}?${params.toString()}`;
}

// Function to populate dropdown with options
function populateDropdown(selectElement, items, placeholder, emptyMessage) {
    selectElement.innerHTML = `<option value="">${placeholder}</option>`;

    if (items && items.length > 0) {
        items.forEach(item => {
            const option = document.createElement('option');
            option.value = item;
            option.textContent = item;
            selectElement.appendChild(option);
        });
        console.log(`Added ${items.length} ${placeholder.toLowerCase()}:`, items);
    } else {
        const option = document.createElement('option');
        option.value = "";
        option.textContent = emptyMessage;
        option.disabled = true;
        option.style.fontStyle = "italic";
        selectElement.appendChild(option);
        console.log(`No ${placeholder.toLowerCase()} found with data`);
    }
}

// Handle customer change - Fetch only models for selected customer
document.getElementById('customer').addEventListener('change', function () {
    const customer = this.value;
    const modelSelect = document.getElementById('model');
    const stationSelect = document.getElementById('station');

    // Reset model and station dropdowns
    modelSelect.innerHTML = '<option value="">All Models</option>';
    stationSelect.innerHTML = '<option value="">All Stations</option>';

    // If no customer selected, reset to default state
    if (!customer) {
        modelSelect.innerHTML = '<option value="">Select customer first</option>';
        stationSelect.innerHTML = '<option value="">Select customer first</option>';
        modelSelect.disabled = true;
        stationSelect.disabled = true;
        console.log('Customer cleared - disabled both model and station dropdowns');
        return;
    } else {
        modelSelect.disabled = false;
        stationSelect.disabled = true; // Stations will be enabled when model is selected
    }

    // Show loading state for models only
    modelSelect.innerHTML = '<option value="">Loading models...</option>';
    modelSelect.disabled = true;

    // Build API URL to fetch models for this customer
    const apiUrl = buildApiUrl('/api/get-models-stations', customer);
    console.log('Fetching models for customer:', customer, 'URL:', apiUrl);

    fetch(apiUrl)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Customer data received:', data);
            console.log(`Found ${data.count.models} models for customer: ${customer}`);

            // Populate models dropdown with only available models
            populateDropdown(
                modelSelect,
                data.models,
                "All Models",
                "No models with data available"
            );

            // Reset stations to default (will be populated when model is selected)
            stationSelect.innerHTML = '<option value="">Select model first</option>';
            stationSelect.disabled = true;
        })
        .catch(error => {
            console.error('Error fetching models for customer:', error);
            modelSelect.innerHTML = '<option value="">Failed to load models</option>';
            stationSelect.innerHTML = '<option value="">Select customer first</option>';
            stationSelect.disabled = true;
        })
        .finally(() => {
            modelSelect.disabled = false;
        });
});

// Handle model change - Fetch only stations for selected customer + model
document.getElementById('model').addEventListener('change', function () {
    const customer = document.getElementById('customer').value;
    // FIXED: was `const model`, then reassigned below (`model = this.value`)
    // in the "no model selected" branch - that throws "Assignment to
    // constant variable" every time someone clears the model dropdown.
    let model = this.value;
    const stationSelect = document.getElementById('station');

    // If no customer selected, can't filter stations
    if (!customer) {
        stationSelect.innerHTML = '<option value="">Select customer first</option>';
        stationSelect.disabled = true;
        console.log('No customer selected - cannot filter stations');
        return;
    }

    // If no model selected, show all stations for the customer
    if (!model) {
        stationSelect.innerHTML = '<option value="">Loading all stations...</option>';
        stationSelect.disabled = true;

        const apiUrl = buildApiUrl('/api/get-models-stations', customer);
        console.log('Fetching all stations for customer:', customer);

        fetch(apiUrl)
            .then(response => response.json())
            .then(data => {
                populateDropdown(
                    stationSelect,
                    data.stations,
                    "All Stations",
                    "No stations with data available"
                );
            })
            .catch(error => {
                console.error('Error fetching all stations:', error);
                stationSelect.innerHTML = '<option value="">Failed to load stations</option>';
            })
            .finally(() => {
                stationSelect.disabled = false;
            });
        return;
    }

    // Show loading state for stations
    stationSelect.innerHTML = '<option value="">Loading stations...</option>';
    stationSelect.disabled = true;

    // Build API URL to fetch stations for this customer + model combination
    const apiUrl = buildApiUrl('/api/get-models-stations', customer, model);
    console.log('Fetching stations for customer:', customer, 'model:', model, 'URL:', apiUrl);

    fetch(apiUrl)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.json();
        })
        .then(data => {
            console.log('Model + Customer data received:', data);
            console.log(`Found ${data.count.stations} stations for customer: ${customer}, model: ${model}`);

            // Populate stations dropdown with only available stations for this model
            populateDropdown(
                stationSelect,
                data.stations,
                "All Stations",
                `No stations for ${model}`
            );
        })
        .catch(error => {
            console.error('Error fetching stations for model:', error);
            stationSelect.innerHTML = '<option value="">Failed to load stations</option>';
        })
        .finally(() => {
            stationSelect.disabled = false;
        });
});

// Submit button handler
const filterSubmitBtn = document.getElementById("filterSubmitBtn");
if (filterSubmitBtn) {
    filterSubmitBtn.addEventListener("click", function (e) {
        e.preventDefault();
        setLoadingState(true);

        const customer = document.getElementById("customer").value;
        const model = document.getElementById("model").value;
        const station = document.getElementById("station").value;

        const params = new URLSearchParams();
        if (customer) params.append("customer", customer);
        if (model) params.append("model", model);
        if (station) params.append("station", station);

        // Preserve existing date filters
        const dateFilters = getCurrentDateFilters();
        if (dateFilters.start_date) params.append("start_date", dateFilters.start_date);
        if (dateFilters.end_date) params.append("end_date", dateFilters.end_date);

        console.log('Submitting filters:', { customer, model, station, ...dateFilters });
        window.location.href = `${getAppOrigin()}/?${params.toString()}`;
    });   // FIXED: this closing brace was missing, which left every handler
}         // below unclosed and made the whole file fail to parse.

// Initialize on page load - UPDATED TO PROPERLY HANDLE DISABLED STATES
window.addEventListener('load', function () {
    setLoadingState(false);

    const dateFilters = getCurrentDateFilters();
    if (dateFilters.start_date || dateFilters.end_date) {
        console.log('Page loaded with date filters:', dateFilters);
    }

    // Initialize dropdown states first
    initializeDropdownStates();
});

// Handle page visibility change
document.addEventListener('visibilitychange', function () {
    if (!document.hidden) {
        setLoadingState(false);
    }
});

// Initialize dropdown states immediately when script loads (before window load event)
document.addEventListener('DOMContentLoaded', function () {
    console.log('DOM loaded - initializing dropdown states');
    initializeDropdownStates();
});

// Reusable function to trigger CSV download
function triggerCsvDownload(startDate, endDate) {
    const params = new URLSearchParams();
    if (startDate) params.append('start_date', startDate);
    if (endDate) params.append('end_date', endDate);

    // Preserve existing filters (customer, model, station)
    const customer = document.getElementById('customer').value;
    const model = document.getElementById('model').value;
    const station = document.getElementById('station').value;
    if (customer) params.append('customer', customer);
    if (model) params.append('model', model);
    if (station) params.append('station', station);

    const downloadUrl = `/api/download_csv?${params.toString()}`;
    console.log('Triggering CSV download with URL:', downloadUrl);

    // Set loading state
    setLoadingState(true);

    fetch(downloadUrl)
        .then(response => {
            if (!response.ok) {
                throw new Error(`HTTP error! status: ${response.status}`);
            }
            return response.blob();
        })
        .then(blob => {
            const url = window.URL.createObjectURL(blob);
            const a = document.createElement('a');
            a.style.display = 'none';
            a.href = url;
            a.download = `production_data.csv`;
            document.body.appendChild(a);
            a.click();
            window.URL.revokeObjectURL(url);
            document.body.removeChild(a);
        })
        .catch(error => {
            console.error('Error downloading CSV:', error);
            alert('Failed to download CSV. Please try again.');
        })
        .finally(() => {
            setLoadingState(false);
        });
}