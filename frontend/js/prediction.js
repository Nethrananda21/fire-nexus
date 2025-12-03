/**
 * FireNexus Prediction Page - Optimized & Clean
 */

const API_BASE = 'http://localhost:8000';

// State
let map = null;
let markersLayer = null;
let predictions = [];

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    initMap();
    loadPredictions();
});

/**
 * Initialize Leaflet map
 */
function initMap() {
    map = L.map('prediction-map', {
        center: [20, 0],
        zoom: 2,
        minZoom: 2,
        maxZoom: 12,
        zoomControl: true
    });

    // Dark themed tiles
    L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
        attribution: '&copy; OpenStreetMap &copy; CARTO',
        subdomains: 'abcd',
        maxZoom: 19
    }).addTo(map);

    // Layer for markers
    markersLayer = L.layerGroup().addTo(map);
}

/**
 * Load predictions from API
 */
async function loadPredictions() {
    showLoading(true);
    setButtonLoading(true);

    try {
        const response = await fetch(`${API_BASE}/api/predictions?limit=100`);
        
        if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
        }

        const data = await response.json();
        // API returns high_risk_zones with lat/lon/probability
        predictions = data.high_risk_zones || [];
        
        updateMap(predictions);
        updateStats(predictions);
        updateTimestamp();

    } catch (error) {
        console.error('Failed to load predictions:', error);
        showError('Failed to load predictions. Is the API running?');
    } finally {
        showLoading(false);
        setButtonLoading(false);
    }
}

/**
 * Update map with prediction markers
 */
function updateMap(preds) {
    markersLayer.clearLayers();

    preds.forEach(pred => {
        // API uses 'probability' and 'lat'/'lon'
        const score = pred.probability || 0;
        const risk = getRiskLevel(score);
        const marker = L.circleMarker([pred.lat, pred.lon], {
            radius: 8 + (score / 15),
            fillColor: risk.color,
            fillOpacity: 0.7,
            color: risk.color,
            weight: 2,
            opacity: 0.9
        });

        marker.bindPopup(createPopup(pred, risk));
        markersLayer.addLayer(marker);
    });

    // Fit to markers if we have any
    if (preds.length > 0 && markersLayer.getLayers().length > 0) {
        try {
            const bounds = L.featureGroup(markersLayer.getLayers()).getBounds();
            if (bounds.isValid()) {
                map.fitBounds(bounds, { padding: [30, 30], maxZoom: 6 });
            }
        } catch (e) {
            console.log('Could not fit bounds:', e);
        }
    }
}

/**
 * Get risk level info from score
 */
function getRiskLevel(score) {
    if (score >= 70) {
        return { level: 'HIGH', color: '#ef4444', class: 'high' };
    } else if (score >= 50) {
        return { level: 'ELEVATED', color: '#f97316', class: 'elevated' };
    } else {
        return { level: 'MEDIUM', color: '#eab308', class: 'medium' };
    }
}

/**
 * Create popup HTML for a prediction
 */
function createPopup(pred, risk) {
    // API uses probability, heuristic_score, ml_score, lat, lon
    const heuristic = pred.heuristic_score?.toFixed(1) || 'N/A';
    const ml = pred.ml_score?.toFixed(1) || 'N/A';
    const combined = pred.probability?.toFixed(1) || 'N/A';

    return `
        <div class="popup-content">
            <div class="popup-header ${risk.class}">
                <i class="ri-fire-fill"></i>
                <h3>${risk.level} RISK</h3>
            </div>
            <div class="popup-stats">
                <div class="popup-stat">
                    <span>Combined Score</span>
                    <span><strong>${combined}%</strong></span>
                </div>
                <div class="popup-stat">
                    <span>Heuristic</span>
                    <span>${heuristic}%</span>
                </div>
                <div class="popup-stat">
                    <span>ML Model</span>
                    <span>${ml}%</span>
                </div>
                <div class="popup-stat">
                    <span>Location</span>
                    <span>${pred.lat.toFixed(2)}°, ${pred.lon.toFixed(2)}°</span>
                </div>
            </div>
            <div class="score-bar">
                <div class="score-fill ${risk.class}" style="width: ${combined}%"></div>
            </div>
        </div>
    `;
}

/**
 * Update stats counters
 */
function updateStats(preds) {
    let high = 0, elevated = 0, medium = 0;

    preds.forEach(p => {
        // API uses 'probability'
        const score = p.probability || 0;
        if (score >= 70) high++;
        else if (score >= 50) elevated++;
        else medium++;
    });

    animateCounter('high-count', high);
    animateCounter('elevated-count', elevated);
    animateCounter('medium-count', medium);
}

/**
 * Simple counter animation
 */
function animateCounter(id, target) {
    const el = document.getElementById(id);
    if (!el) return;

    const duration = 500;
    const start = parseInt(el.textContent) || 0;
    const startTime = performance.now();

    function update(currentTime) {
        const elapsed = currentTime - startTime;
        const progress = Math.min(elapsed / duration, 1);
        
        const current = Math.floor(start + (target - start) * progress);
        el.textContent = current;

        if (progress < 1) {
            requestAnimationFrame(update);
        }
    }

    requestAnimationFrame(update);
}

/**
 * Update last update timestamp
 */
function updateTimestamp() {
    const el = document.getElementById('last-update');
    if (el) {
        el.textContent = `Updated: ${new Date().toLocaleTimeString()}`;
    }
}

/**
 * Show/hide loading overlay
 */
function showLoading(show) {
    const overlay = document.getElementById('loading');
    if (overlay) {
        overlay.classList.toggle('active', show);
    }
}

/**
 * Set button loading state
 */
function setButtonLoading(loading) {
    const btn = document.getElementById('refresh-btn');
    if (!btn) return;

    btn.disabled = loading;
    const icon = btn.querySelector('i');
    if (icon) {
        icon.classList.toggle('spin', loading);
    }
}

/**
 * Show error message
 */
function showError(message) {
    const el = document.getElementById('last-update');
    if (el) {
        el.textContent = message;
        el.style.color = '#ef4444';
        setTimeout(() => {
            el.style.color = '';
        }, 5000);
    }
}
