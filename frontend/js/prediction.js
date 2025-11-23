// Configuration
const API_BASE_URL = 'http://localhost:8000';
const CACHE_DURATION = 2 * 60 * 1000; // 2 minutes cache validity

// Initialize Map
const predMap = L.map('predictionMap', {
    zoomControl: false,
    attributionControl: false
}).setView([20, 0], 3);

L.control.zoom({ position: 'topright' }).addTo(predMap);
L.control.attribution({ position: 'bottomright' }).addTo(predMap);

// Dark Matter Basemap
L.tileLayer('https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png', {
    attribution: '&copy; OpenStreetMap &copy; CARTO',
    subdomains: 'abcd',
    maxZoom: 19
}).addTo(predMap);

// Layer Groups for different risk levels
let highRiskLayer = L.layerGroup().addTo(predMap);
let mediumRiskLayer = L.layerGroup().addTo(predMap);
let lowRiskLayer = L.layerGroup().addTo(predMap);

let allPredictions = [];

// Load predictions on page load
window.addEventListener('load', () => {
    loadPredictions();
});

// Listen to filter changes
document.getElementById('riskFilter')?.addEventListener('change', updateMapFilters);

async function loadPredictions() {
    const btn = document.querySelector('.btn-primary');
    const originalBtnText = btn.innerHTML;
    btn.innerHTML = '<i class="ri-loader-4-line" style="animation: spin 1s infinite"></i> Loading...';
    btn.disabled = true;

    try {
        // Check cache first
        const cachedPredictions = getCachedData('predictionData');
        
        if (cachedPredictions) {
            console.log('Using cached prediction data');
            allPredictions = cachedPredictions.filter(f => f.prediction_probability !== null);
            updatePredictionStats(allPredictions);
            updateMap(allPredictions);
            document.getElementById('predictionUpdate').innerHTML = 
                `Last updated: ${new Date().toLocaleTimeString()} (cached)`;
            btn.innerHTML = originalBtnText;
            btn.disabled = false;
            return;
        }

        // Fetch fires with predictions
        const response = await fetch(`${API_BASE_URL}/api/fires?limit=50000&include_predictions=true`);
        const fires = await response.json();

        // Filter fires with predictions
        allPredictions = fires.filter(f => f.prediction_probability !== null);

        // Cache the data
        setCachedData('predictionData', allPredictions);

        updatePredictionStats(allPredictions);
        updateMap(allPredictions);

        document.getElementById('predictionUpdate').innerHTML = 
            `Last updated: ${new Date().toLocaleTimeString()}`;

    } catch (error) {
        console.error('Error loading predictions:', error);
        alert('Failed to load predictions. Is the backend running?');
    } finally {
        btn.innerHTML = originalBtnText;
        btn.disabled = false;
    }
}

function updatePredictionStats(predictions) {
    const high = predictions.filter(p => p.prediction_risk_level === 'HIGH').length;
    const medium = predictions.filter(p => p.prediction_risk_level === 'MEDIUM').length;
    const low = predictions.filter(p => p.prediction_risk_level === 'LOW').length;

    animateValue('highRiskCount', 
        parseInt(document.getElementById('highRiskCount').innerText) || 0, 
        high, 800);
    animateValue('mediumRiskCount', 
        parseInt(document.getElementById('mediumRiskCount').innerText) || 0, 
        medium, 800);
    animateValue('lowRiskCount', 
        parseInt(document.getElementById('lowRiskCount').innerText) || 0, 
        low, 800);
}

function updateMap(predictions) {
    // Clear existing layers
    highRiskLayer.clearLayers();
    mediumRiskLayer.clearLayers();
    lowRiskLayer.clearLayers();

    // Apply filter
    const riskFilter = document.getElementById('riskFilter').value;
    let filtered = predictions;
    if (riskFilter !== 'all') {
        filtered = predictions.filter(p => p.prediction_risk_level === riskFilter);
    }

    // Add markers based on risk level
    filtered.forEach(fire => {
        let marker;
        let layer;

        if (fire.prediction_risk_level === 'HIGH') {
            marker = L.circleMarker([fire.latitude, fire.longitude], {
                radius: 10,
                fillColor: '#ef4444',
                color: '#fff',
                weight: 2,
                opacity: 0.8,
                fillOpacity: 0.9,
                className: 'pulse-marker'
            });
            layer = highRiskLayer;
        } else if (fire.prediction_risk_level === 'MEDIUM') {
            marker = L.circleMarker([fire.latitude, fire.longitude], {
                radius: 8,
                fillColor: '#f59e0b',
                color: '#fff',
                weight: 2,
                opacity: 0.6,
                fillOpacity: 0.8
            });
            layer = mediumRiskLayer;
        } else {
            marker = L.circleMarker([fire.latitude, fire.longitude], {
                radius: 6,
                fillColor: '#10b981',
                color: 'transparent',
                weight: 0,
                fillOpacity: 0.7
            });
            layer = lowRiskLayer;
        }

        marker.bindPopup(createPredictionPopup(fire));
        layer.addLayer(marker);
    });
}

function updateMapFilters() {
    updateMap(allPredictions);
}

function createPredictionPopup(fire) {
    const probPercent = (fire.prediction_probability * 100).toFixed(1);
    const riskClass = fire.prediction_risk_level === 'HIGH' ? 'risk-high' : 
                     fire.prediction_risk_level === 'MEDIUM' ? 'risk-medium' : 'risk-low';

    return `
        <div class="popup-content">
            <h3><i class="ri-brain-line"></i> AI FIRE PREDICTION</h3>
            
            <div class="popup-section">
                <h4><i class="ri-fire-fill"></i> Fire Information</h4>
                <div class="popup-row">
                    <span class="popup-label">Severity</span>
                    <strong>${fire.severity.toUpperCase()}</strong>
                </div>
                <div class="popup-row">
                    <span class="popup-label">FRP Intensity</span>
                    <strong>${fire.frp ? fire.frp.toFixed(1) + ' MW' : 'N/A'}</strong>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Confidence</span>
                    <strong>${fire.confidence === 'h' ? 'High' : fire.confidence === 'n' ? 'Nominal' : 'Low'}</strong>
                </div>
            </div>

            <div class="popup-divider"></div>
            
            <div class="popup-section">
                <h4><i class="ri-line-chart-line"></i> ML Prediction</h4>
                <div class="popup-row">
                    <span class="popup-label">Continuation Probability</span>
                    <strong style="font-size: 16px; color: var(--primary);">${probPercent}%</strong>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Risk Level</span>
                    <strong class="${riskClass}" style="font-size: 15px;">${fire.prediction_risk_level}</strong>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Will Continue?</span>
                    <strong>${probPercent > 50 ? 'Likely' : 'Unlikely'}</strong>
                </div>
            </div>

            <div class="popup-divider"></div>

            <div class="popup-section">
                <div class="popup-row">
                    <span class="popup-label">Coordinates</span>
                    <strong>${fire.latitude.toFixed(3)}, ${fire.longitude.toFixed(3)}</strong>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Satellite</span>
                    <strong>${fire.satellite || 'N/A'}</strong>
                </div>
                <div class="popup-row">
                    <span class="popup-label">Detected</span>
                    <strong>${new Date(fire.detected_at).toLocaleString()}</strong>
                </div>
            </div>
        </div>
    `;
}

function animateValue(id, start, end, duration) {
    const element = document.getElementById(id);
    if (!element) return;

    const range = end - start;
    const increment = range / (duration / 16);
    let current = start;

    const timer = setInterval(() => {
        current += increment;
        if ((increment > 0 && current >= end) || (increment < 0 && current <= end)) {
            current = end;
            clearInterval(timer);
        }
        element.textContent = Math.round(current).toLocaleString();
    }, 16);
}

// Add CSS for animation
const style = document.createElement('style');
style.textContent = `
    @keyframes spin {
        from { transform: rotate(0deg); }
        to { transform: rotate(360deg); }
    }
`;
document.head.appendChild(style);

// Cache Management Functions
function setCachedData(key, data) {
    const cacheItem = {
        data: data,
        timestamp: Date.now()
    };
    try {
        const serialized = JSON.stringify(cacheItem);
        
        // Check if data is too large (>4MB)
        if (serialized.length > 4 * 1024 * 1024) {
            console.warn('Data too large to cache, skipping cache');
            return;
        }
        
        localStorage.setItem(key, serialized);
    } catch (e) {
        if (e.name === 'QuotaExceededError') {
            console.warn('LocalStorage quota exceeded, clearing cache and retrying');
            clearAllCache();
            try {
                // Retry with smaller dataset
                const smallerData = data.slice(0, Math.floor(data.length / 2));
                const smallerCache = {
                    data: smallerData,
                    timestamp: Date.now(),
                    partial: true
                };
                localStorage.setItem(key, JSON.stringify(smallerCache));
                console.log(`Cached ${smallerData.length} items (reduced from ${data.length})`);
            } catch (retryError) {
                console.error('Failed to cache even after clearing:', retryError);
            }
        } else {
            console.error('Error setting cache:', e);
        }
    }
}

function getCachedData(key) {
    try {
        const cached = localStorage.getItem(key);
        if (!cached) return null;

        const cacheItem = JSON.parse(cached);
        const age = Date.now() - cacheItem.timestamp;

        // Check if cache is still valid
        if (age < CACHE_DURATION) {
            if (cacheItem.partial) {
                console.log('Using partial cached data');
            }
            return cacheItem.data;
        }

        // Cache expired, remove it
        localStorage.removeItem(key);
        return null;
    } catch (e) {
        console.error('Error reading cache:', e);
        localStorage.removeItem(key);
        return null;
    }
}

function clearAllCache() {
    try {
        localStorage.removeItem('fireData');
        localStorage.removeItem('fireStats');
        localStorage.removeItem('predictionData');
        console.log('Cache cleared successfully');
    } catch (e) {
        console.error('Error clearing cache:', e);
    }
}

function clearOldCache() {
    clearAllCache();
}

function clearCache() {
    clearAllCache();
    console.log('Manual cache clear');
    loadPredictions();
}
