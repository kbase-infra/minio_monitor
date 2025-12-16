// State
let currentData = [];
let quotaGB = 250;
let sortColumn = 2;
let sortAsc = false;
let files = [];

// DOM Elements
const fileButtonsContainer = document.getElementById('file-buttons');
const fileInfo = document.getElementById('file-info');
const tableBody = document.getElementById('table-body');
const searchBox = document.getElementById('search-box');
const quotaInput = document.getElementById('quota-input');
const totalDirsEl = document.getElementById('total-dirs');
const totalSizeEl = document.getElementById('total-size');
const overQuotaEl = document.getElementById('over-quota');

// Initialize
async function init() {
    // Load file list
    const response = await fetch('api/files');
    const data = await response.json();
    files = data.files;

    // Render file buttons
    fileButtonsContainer.innerHTML = files.map((file, index) => `
        <button class="file-btn" data-filename="${file.name}" id="btn-${index}">
            ${file.name.split('/').pop()}
        </button>
    `).join('');

    // Add click listeners to buttons
    fileButtonsContainer.querySelectorAll('.file-btn').forEach(btn => {
        btn.addEventListener('click', () => loadFile(btn.dataset.filename, btn));
    });

    // Load first file
    if (files.length > 0) {
        const firstBtn = document.getElementById('btn-0');
        loadFile(files[0].name, firstBtn);
    }

    // Set up event listeners
    searchBox.addEventListener('keyup', filterTable);
    quotaInput.addEventListener('change', applyQuota);

    // Set up table header sorting
    document.querySelectorAll('th[data-column]').forEach(th => {
        th.addEventListener('click', () => sortTable(parseInt(th.dataset.column)));
    });
}

async function loadFile(filename, btn) {
    // Update active button
    document.querySelectorAll('.file-btn').forEach(b => b.classList.remove('active'));
    if (btn) btn.classList.add('active');

    const response = await fetch(`api/csv?file=${encodeURIComponent(filename)}`);
    const data = await response.json();
    currentData = data.rows;

    fileInfo.textContent = `Loaded: ${filename} (${data.rows.length} rows)`;

    renderTable();
    updateStats();
}

function renderTable() {
    const searchTerm = searchBox.value.toLowerCase();

    // Filter data
    let filtered = currentData.filter(row =>
        row.path.toLowerCase().includes(searchTerm)
    );

    // Sort data
    filtered.sort((a, b) => {
        let valA, valB;
        if (sortColumn === 0) {
            valA = a.path;
            valB = b.path;
        } else if (sortColumn === 1 || sortColumn === 2) {
            valA = parseFloat(a.size_gb);
            valB = parseFloat(b.size_gb);
        } else {
            valA = parseFloat(a.size_mb);
            valB = parseFloat(b.size_mb);
        }

        if (sortAsc) {
            return valA > valB ? 1 : -1;
        } else {
            return valA < valB ? 1 : -1;
        }
    });

    // Find max size for bar scaling
    const maxSize = Math.max(...filtered.map(row => parseFloat(row.size_gb)));

    tableBody.innerHTML = filtered.map(row => {
        const sizeGB = parseFloat(row.size_gb);
        const barWidth = (sizeGB / maxSize) * 100;
        const isOverQuota = sizeGB > quotaGB && row.path.includes('/');

        return `
            <tr class="${isOverQuota ? 'over-quota' : ''}">
                <td>${row.path}</td>
                <td>
                    <span class="size-bar" style="width: ${barWidth}px"></span>
                    ${row.size_human}
                </td>
                <td>${parseFloat(row.size_gb).toFixed(2)}</td>
                <td>${parseFloat(row.size_mb).toFixed(2)}</td>
            </tr>
        `;
    }).join('');
}

function updateStats() {
    const dirs = currentData.filter(row => row.path.includes('/'));
    const totalSize = dirs.reduce((sum, row) => sum + parseFloat(row.size_gb), 0);
    const overQuota = dirs.filter(row => parseFloat(row.size_gb) > quotaGB).length;

    totalDirsEl.textContent = dirs.length;
    totalSizeEl.textContent = totalSize.toFixed(2) + ' GB';
    overQuotaEl.textContent = overQuota;
}

function filterTable() {
    renderTable();
}

function sortTable(column) {
    if (sortColumn === column) {
        sortAsc = !sortAsc;
    } else {
        sortColumn = column;
        sortAsc = false;
    }
    renderTable();
}

function applyQuota() {
    quotaGB = parseFloat(quotaInput.value) || 250;
    renderTable();
    updateStats();
}

// Initialize on page load
document.addEventListener('DOMContentLoaded', init);