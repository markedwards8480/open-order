
const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const session = require('express-session');
const cron = require('node-cron');
require('dotenv').config();

// All data is stored server-side in PostgreSQL:
// - Order data in order_items and sales_orders tables
// - Images cached in image_cache table (fetched once from Zoho, then stored)
// - Zoho tokens in zoho_tokens table
// No local filesystem or browser caching is used for persistence

const app = express();
const PORT = process.env.PORT || 3001;

// --- Origin Protection ---
const ORIGIN_SECRET = process.env.ORIGIN_SECRET;
if (ORIGIN_SECRET) {
      app.use((req, res, next) => {
              if (req.headers['x-origin-secret'] === ORIGIN_SECRET) {
                        return next();
              }
              res.status(403).json({ error: 'Direct access not allowed' });
      });
}

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ extended: true, limit: '50mb' }));
app.use(session({
    secret: process.env.SESSION_SECRET || 'open-orders-secret-key',
    resave: false,
    saveUninitialized: false,
    cookie: { secure: false, maxAge: 24 * 60 * 60 * 1000 }
}));

const upload = multer({ storage: multer.memoryStorage() });

var zohoAccessToken = null;

// ============================================
// ADMIN PANEL INTEGRATION
// ============================================
const ADMIN_PANEL_URL = process.env.ADMIN_PANEL_URL;
const ADMIN_PANEL_API_KEY = process.env.ADMIN_PANEL_API_KEY;

// Helper: Report data freshness to admin panel
async function reportDataFreshness(dataSource, recordCount, notes) {
    if (!ADMIN_PANEL_URL || !ADMIN_PANEL_API_KEY) {
        console.log('Admin panel not configured, skipping freshness report');
        return;
    }
    try {
        await fetch(`${ADMIN_PANEL_URL}/api/health/freshness`, {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': ADMIN_PANEL_API_KEY
            },
            body: JSON.stringify({
                data_source: dataSource,
                record_count: recordCount,
                notes: notes || null
            })
        });
        console.log('Reported freshness to admin panel:', dataSource, recordCount, 'records');
    } catch (err) {
        console.error('Failed to report data freshness:', err.message);
    }
}

// ============================================
// ZOHO ANALYTICS CONFIGURATION
// ============================================
var ZOHO_ANALYTICS_ORG_ID = process.env.ZOHO_ANALYTICS_ORG_ID || '677679394';
var ZOHO_ANALYTICS_WORKSPACE_ID = process.env.ZOHO_ANALYTICS_WORKSPACE_ID || '1746857000002490128';
var ZOHO_ANALYTICS_VIEW_ID = process.env.ZOHO_ANALYTICS_VIEW_ID || '1746857000094557183';

// Fetch data from Zoho Analytics using async bulk export
async function fetchZohoAnalyticsData() {
    if (!zohoAccessToken) {
        var tokenResult = await refreshZohoToken();
        if (!tokenResult.success) {
            return { success: false, error: 'Failed to get Zoho token: ' + tokenResult.error };
        }
    }

    try {
        var headers = {
            'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken
        };
        if (ZOHO_ANALYTICS_ORG_ID && /^\d+$/.test(ZOHO_ANALYTICS_ORG_ID)) {
            headers['ZANALYTICS-ORGID'] = ZOHO_ANALYTICS_ORG_ID;
        }

        // Use bulk async export API (required for large views)
        var config = JSON.stringify({
            outputConfig: {
                outputFormat: 'JSON'
            }
        });
        var exportUrl = 'https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/' + ZOHO_ANALYTICS_WORKSPACE_ID + '/views/' + ZOHO_ANALYTICS_VIEW_ID + '/data?CONFIG=' + encodeURIComponent(config);

        console.log('Initiating bulk export from Zoho Analytics:', exportUrl);

        var response = await fetch(exportUrl, {
            method: 'GET',
            headers: headers
        });

        // If token expired, refresh and retry
        if (response.status === 401) {
            console.log('Token expired, refreshing...');
            await refreshZohoToken();
            headers['Authorization'] = 'Zoho-oauthtoken ' + zohoAccessToken;
            response = await fetch(exportUrl, {
                method: 'GET',
                headers: headers
            });
        }

        if (!response.ok) {
            var errorText = await response.text();
            console.error('Zoho Analytics bulk export error:', response.status, errorText);
            return { success: false, error: 'API error: ' + response.status + ' - ' + errorText };
        }

        var initData = await response.json();
        console.log('Bulk export response:', JSON.stringify(initData).substring(0, 500));

        // Check if we got data directly (small dataset) or a job ID (large dataset)
        if (initData.data && Array.isArray(initData.data) && initData.column_order) {
            // Direct response with data
            console.log('Got direct data response with', initData.data.length, 'rows');
            return { success: true, data: initData };
        }

        // Check for job-based response
        var jobId = initData.data && initData.data.jobId;
        if (!jobId) {
            // Maybe the data is in a different format
            if (initData.data) {
                return { success: true, data: initData };
            }
            return { success: false, error: 'Unexpected response format: ' + JSON.stringify(initData).substring(0, 200) };
        }

        // Poll for job completion
        console.log('Got job ID:', jobId, '- polling for completion');
        var jobUrl = 'https://analyticsapi.zoho.com/restapi/v2/bulk/workspaces/' + ZOHO_ANALYTICS_WORKSPACE_ID + '/views/' + ZOHO_ANALYTICS_VIEW_ID + '/jobs/' + jobId;
        var maxAttempts = 60;
        var pollInterval = 3000;

        for (var attempt = 0; attempt < maxAttempts; attempt++) {
            await new Promise(function(resolve) { setTimeout(resolve, pollInterval); });

            var statusResponse = await fetch(jobUrl, {
                method: 'GET',
                headers: headers
            });

            if (!statusResponse.ok) {
                console.log('Job status check failed, attempt', attempt);
                continue;
            }

            var statusData = await statusResponse.json();
            console.log('Job status:', JSON.stringify(statusData).substring(0, 300));

            var jobInfo = statusData.data || statusData;
            var jobStatus = jobInfo.jobStatus;

            if (jobStatus === 'JOB COMPLETED') {
                // Download the exported data
                var downloadUrl = jobInfo.downloadUrl;
                if (downloadUrl) {
                    console.log('Downloading from:', downloadUrl);
                    var downloadResponse = await fetch(downloadUrl, {
                        method: 'GET',
                        headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
                    });

                    if (downloadResponse.ok) {
                        var data = await downloadResponse.json();
                        console.log('Downloaded', data.data ? data.data.length : 0, 'rows');
                        return { success: true, data: data };
                    }
                }
                // No download URL, check if data is in the response
                if (jobInfo.data) {
                    return { success: true, data: jobInfo };
                }
                return { success: false, error: 'Job completed but no data available' };
            } else if (jobStatus === 'JOB FAILED' || jobStatus === 'FAILED') {
                return { success: false, error: 'Export job failed: ' + (jobInfo.errorMessage || 'Unknown error') };
            }
            // Still in progress, continue polling
            console.log('Job still in progress, attempt', attempt + 1);
        }

        return { success: false, error: 'Export job timed out after ' + maxAttempts + ' attempts' };
    } catch (err) {
        console.error('Error fetching Zoho Analytics:', err);
        return { success: false, error: err.message };
    }
}

// Process and import Zoho Analytics data
async function syncFromZohoAnalytics() {
    console.log('Starting Zoho Analytics sync...');

    var result = await fetchZohoAnalyticsData();
    if (!result.success) {
        return result;
    }

    var data = result.data;
    if (!data || !data.data || !Array.isArray(data.data)) {
        return { success: false, error: 'Invalid data format from Zoho Analytics' };
    }

    var rows = data.data;
    var columns = data.column_order || [];

    console.log('Received ' + rows.length + ' rows with columns:', columns);

    if (rows.length === 0) {
        return { success: false, error: 'No data returned from Zoho Analytics' };
    }

    // Build column index map (case-insensitive)
    var colMap = {};
    columns.forEach(function(col, idx) {
        colMap[col.toLowerCase().replace(/[^a-z0-9_]/g, '_')] = idx;
    });

    // Find columns using flexible matching
    function findCol(possibleNames) {
        for (var i = 0; i < possibleNames.length; i++) {
            var name = possibleNames[i].toLowerCase().replace(/[^a-z0-9_]/g, '_');
            if (colMap[name] !== undefined) return colMap[name];
        }
        // Partial match
        for (var i = 0; i < possibleNames.length; i++) {
            var partial = possibleNames[i].toLowerCase();
            for (var key in colMap) {
                if (key.indexOf(partial) !== -1) return colMap[key];
            }
        }
        return -1;
    }

    var mapping = {
        so_number: findCol(['sales_order_number', 'so_number', 'so', 'sales_order', 'order_number']),
        customer: findCol(['customer_name', 'customer', 'cust', 'buyer']),
        style_number: findCol(['style_name', 'style_number', 'style', 'sku', 'item_number']),
        style_name: findCol(['style_suffix', 'description', 'item_name', 'product_name']),
        commodity: findCol(['commodity', 'copmmodity', 'category', 'product_type']),
        color: findCol(['color', 'colour', 'color_name']),
        quantity: findCol(['quantity', 'qty', 'units', 'order_qty']),
        unit_price: findCol(['so_price_fcy', 'so_price_bcy', 'unit_price', 'price']),
        total_amount: findCol(['total_fcy', 'total_bcy', 'total_amount', 'total', 'amount']),
        delivery_date: findCol(['so_cancel_date', 'delivery_date', 'ship_date', 'due_date']),
        status: findCol(['so_status', 'status', 'order_status']),
        image_url: findCol(['style_image', 'image_url', 'image', 'workdrive_link']),
        po_number: findCol(['purchase_order_number', 'po_number', 'po']),
        vendor_name: findCol(['vendor_name', 'vendor', 'supplier', 'factory']),
        po_status: findCol(['po_status', 'purchase_status']),
        po_quantity: findCol(['po_quantity', 'po_qty']),
        po_unit_price: findCol(['po_price', 'po_unit_price', 'po_price_fcy']),
        po_total: findCol(['po_total_fcy', 'po_total', 'po_amount']),
        po_warehouse_date: findCol(['in_warehouse_date', 'po_warehouse_date', 'warehouse_date'])
    };

    console.log('Column mapping:', mapping);

    if (mapping.so_number === -1 || mapping.customer === -1 || mapping.style_number === -1) {
        return { success: false, error: 'Missing required columns. Found columns: ' + columns.join(', ') };
    }

    // Clear existing data
    await pool.query('TRUNCATE TABLE order_items RESTART IDENTITY CASCADE');
    await pool.query('TRUNCATE TABLE sales_orders RESTART IDENTITY CASCADE');
    console.log('Cleared existing data');

    var imported = 0;
    var errors = [];

    for (var i = 0; i < rows.length; i++) {
        try {
            var row = rows[i];
            var getValue = function(idx) { return idx >= 0 && row[idx] !== undefined ? String(row[idx]).trim() : ''; };
            var getNumber = function(idx) {
                var val = getValue(idx).replace(/[^0-9.-]/g, '');
                return parseFloat(val) || 0;
            };
            var getDate = function(idx) {
                var val = getValue(idx);
                if (!val) return null;
                var d = new Date(val);
                return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
            };

            var soNumber = getValue(mapping.so_number);
            var customer = getValue(mapping.customer);
            var styleNumber = getValue(mapping.style_number);

            if (!soNumber || !customer || !styleNumber) continue;

            var styleName = getValue(mapping.style_name) || styleNumber;
            var commodity = getValue(mapping.commodity) || '';
            var color = getValue(mapping.color) || '';
            var quantity = getNumber(mapping.quantity);
            var unitPrice = getNumber(mapping.unit_price);
            var totalAmount = getNumber(mapping.total_amount) || (quantity * unitPrice);
            var deliveryDate = getDate(mapping.delivery_date);
            var status = getValue(mapping.status) || 'Open';
            var imageUrl = getValue(mapping.image_url) || '';

            // Normalize status
            var statusLower = status.toLowerCase();
            if (statusLower.includes('partial')) status = 'Partial';
            else if (statusLower.includes('invoice') || statusLower.includes('shipped') || statusLower.includes('complete') || statusLower.includes('billed')) status = 'Invoiced';
            else status = 'Open';

            // PO data
            var poNumber = getValue(mapping.po_number);
            var vendorName = getValue(mapping.vendor_name);
            var poStatus = getValue(mapping.po_status);
            var poQuantity = getNumber(mapping.po_quantity);
            var poUnitPrice = getNumber(mapping.po_unit_price);
            var poTotal = getNumber(mapping.po_total);
            var poWarehouseDate = getDate(mapping.po_warehouse_date);

            // Insert into order_items
            await pool.query(`
                INSERT INTO order_items (so_number, customer, style_number, style_name, commodity, color, quantity, unit_price, total_amount, delivery_date, status, image_url, po_number, vendor_name, po_status, po_quantity, po_unit_price, po_total, po_warehouse_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            `, [soNumber, customer, styleNumber, styleName, commodity, color, quantity, unitPrice, totalAmount, deliveryDate, status, imageUrl, poNumber, vendorName, poStatus, poQuantity, poUnitPrice, poTotal, poWarehouseDate]);

            imported++;
        } catch (err) {
            errors.push('Row ' + i + ': ' + err.message);
            if (errors.length > 10) break;
        }
    }

    // Log import
    await pool.query('INSERT INTO import_history (import_type, status, records_imported) VALUES ($1, $2, $3)',
        ['zoho_analytics_sync', 'success', imported]);

    // Report freshness to admin panel
    await reportDataFreshness('Open Orders', imported, 'Zoho Analytics sync');

    console.log('Zoho Analytics sync complete: ' + imported + ' records imported');
    return { success: true, imported: imported, errors: errors };
}

// ============================================
// WORKDRIVE FOLDER SYNC - Auto-sync from WorkDrive folder
// ============================================

// WorkDrive folder configuration - Catalog Files/Open Sales Query
// Same folder contains both Sales Orders CSV and Import POs CSV
var WORKDRIVE_SYNC_FOLDER_ID = process.env.WORKDRIVE_SYNC_FOLDER_ID || '';
var WORKDRIVE_IMPORT_PO_FOLDER_ID = WORKDRIVE_SYNC_FOLDER_ID; // Uses same folder
var WORKDRIVE_TEAM_ID = process.env.WORKDRIVE_TEAM_ID || '';

// List files in a WorkDrive folder - tries multiple API endpoints
async function listWorkDriveFolder(folderId, teamId) {
    if (!zohoAccessToken) {
        var tokenResult = await refreshZohoToken(true);
        if (!tokenResult.success) {
            return { success: false, error: 'Failed to get Zoho token: ' + tokenResult.error };
        }
    }

    // Use provided teamId or fall back to env variable
    var effectiveTeamId = teamId || WORKDRIVE_TEAM_ID;

    try {
        // Try multiple API endpoint formats and header combinations
        var attempts = [
            { url: 'https://workdrive.zoho.com/api/v1/files/' + folderId + '/files', headers: {} },
            { url: 'https://workdrive.zoho.com/api/v1/files/' + folderId + '/files?page=1&per_page=50', headers: {} },
            { url: 'https://workdrive.zoho.com/api/v1/teamfolders/' + folderId + '/files', headers: {} },
            { url: 'https://www.zohoapis.com/workdrive/api/v1/files/' + folderId + '/files', headers: {} },
            { url: 'https://workdrive.zoho.com/api/v1/files?parent_id=' + folderId, headers: {} }
        ];

        // Add team header variations
        if (effectiveTeamId) {
            attempts.forEach(function(a) {
                a.headers['x-team-id'] = effectiveTeamId;
            });
            // Also try with ZWORKDRIVE-TEAMID header
            attempts.push({
                url: 'https://workdrive.zoho.com/api/v1/files/' + folderId + '/files',
                headers: { 'ZWORKDRIVE-TEAMID': effectiveTeamId }
            });
        }

        var response = null;
        var lastError = null;
        var lastErrorText = null;

        for (var i = 0; i < attempts.length; i++) {
            var attempt = attempts[i];
            console.log('Trying:', attempt.url);

            var headers = Object.assign({ 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }, attempt.headers);

            response = await fetch(attempt.url, { headers: headers });

            if (response.status === 401) {
                await refreshZohoToken(true);
                headers['Authorization'] = 'Zoho-oauthtoken ' + zohoAccessToken;
                response = await fetch(attempt.url, { headers: headers });
            }

            if (response.ok) {
                console.log('SUCCESS with:', attempt.url);
                break;
            } else {
                lastError = response.status;
                try {
                    lastErrorText = await response.text();
                    console.log('Failed:', response.status, lastErrorText.substring(0, 300));
                } catch(e) {
                    console.log('Failed:', response.status);
                }
                response = null; // Reset for next attempt
            }
        }

        if (!response || !response.ok) {
            return { success: false, error: 'WorkDrive API error: ' + lastError + (lastErrorText ? ' - ' + lastErrorText.substring(0, 100) : '') };
        }

        var data = await response.json();
        return { success: true, files: data.data || [] };
    } catch (err) {
        console.error('listWorkDriveFolder error:', err);
        return { success: false, error: err.message };
    }
}

// Find the latest CSV file in the folder (by filename date or modified date)
function findLatestCSV(files) {
    var csvFiles = files.filter(function(f) {
        return f.attributes && f.attributes.name && f.attributes.name.toLowerCase().endsWith('.csv');
    });

    if (csvFiles.length === 0) return null;

    // Sort by modified date (newest first)
    csvFiles.sort(function(a, b) {
        var dateA = new Date(a.attributes.modified_time || 0);
        var dateB = new Date(b.attributes.modified_time || 0);
        return dateB - dateA;
    });

    return csvFiles[0];
}

// Download a file from WorkDrive
async function downloadWorkDriveFile(fileId) {
    if (!zohoAccessToken) {
        var tokenResult = await refreshZohoToken(true);
        if (!tokenResult.success) {
            return { success: false, error: 'Failed to get Zoho token: ' + tokenResult.error };
        }
    }

    try {
        var url = 'https://workdrive.zoho.com/api/v1/download/' + fileId;
        var response = await fetch(url, {
            headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
        });

        if (response.status === 401) {
            await refreshZohoToken(true);
            response = await fetch(url, {
                headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
            });
        }

        if (!response.ok) {
            return { success: false, error: 'Download failed: ' + response.status };
        }

        var text = await response.text();
        return { success: true, content: text };
    } catch (err) {
        return { success: false, error: err.message };
    }
}

// Sync data from WorkDrive folder
async function syncFromWorkDriveFolder(force) {
    console.log('Starting WorkDrive folder sync... (force=' + !!force + ')');

    if (!WORKDRIVE_SYNC_FOLDER_ID) {
        return { success: false, error: 'WorkDrive folder ID not configured' };
    }

    // List files in folder
    var listResult = await listWorkDriveFolder(WORKDRIVE_SYNC_FOLDER_ID);
    if (!listResult.success) {
        return listResult;
    }

    // Find latest CSV
    var latestFile = findLatestCSV(listResult.files);
    if (!latestFile) {
        return { success: false, error: 'No CSV files found in folder' };
    }

    var fileName = latestFile.attributes.name;
    var fileId = latestFile.id;
    var modifiedTime = latestFile.attributes.modified_time;

    console.log('Found latest file: ' + fileName + ' (modified: ' + modifiedTime + ')');

    // Check if we already imported this file (skip check if force=true or last import had 0 records)
    if (!force) {
        var lastImport = await pool.query(
            "SELECT * FROM import_history WHERE import_type = 'workdrive_sync' AND error_message = $1 ORDER BY created_at DESC LIMIT 1",
            [fileName]
        );
        if (lastImport.rows.length > 0) {
            var lastImportTime = new Date(lastImport.rows[0].created_at);
            var fileModTime = new Date(modifiedTime);
            var lastRecords = lastImport.rows[0].records_imported || 0;
            // Skip only if file hasn't changed AND last import actually got records
            if (fileModTime <= lastImportTime && lastRecords > 0) {
                return { success: true, message: 'File already imported: ' + fileName, skipped: true };
            }
        }
    }

    // Download the file
    console.log('Downloading file: ' + fileName);
    var downloadResult = await downloadWorkDriveFile(fileId);
    if (!downloadResult.success) {
        return downloadResult;
    }

    // Parse CSV
    var lines = downloadResult.content.split('\n');
    var headers = [];
    var rows = [];

    for (var i = 0; i < lines.length; i++) {
        var line = lines[i].trim();
        if (!line) continue;

        // Simple CSV parsing (handles basic cases)
        var values = [];
        var inQuotes = false;
        var currentValue = '';
        for (var j = 0; j < line.length; j++) {
            var char = line[j];
            if (char === '"') {
                inQuotes = !inQuotes;
            } else if (char === ',' && !inQuotes) {
                values.push(currentValue.trim());
                currentValue = '';
            } else {
                currentValue += char;
            }
        }
        values.push(currentValue.trim());

        if (i === 0) {
            headers = values.map(function(h) { return h.toLowerCase().replace(/[^a-z0-9_]/g, '_'); });
        } else {
            var row = {};
            headers.forEach(function(h, idx) { row[h] = values[idx] || ''; });
            rows.push(row);
        }
    }

    console.log('Parsed ' + rows.length + ' rows from CSV');

    // Import the data (same logic as Zoho Analytics sync)
    var imported = 0;
    var errors = [];

    // Clear existing data but KEEP image_cache (same as manual CSV upload)
    await pool.query('TRUNCATE TABLE order_items RESTART IDENTITY CASCADE');
    await pool.query('TRUNCATE TABLE sales_orders RESTART IDENTITY CASCADE');
    console.log('Cleared existing order data (keeping image cache)');

    // Column mapping (flexible matching)
    function findCol(possibleNames) {
        for (var name of possibleNames) {
            if (headers.includes(name)) return name;
        }
        return null;
    }

    var mapping = {
        so_number: findCol(['sales_order_number', 'so_number', 'salesorder_number', 'order_number']),
        customer: findCol(['customer_name', 'customer', 'account_name', 'client']),
        style_number: findCol(['style_name', 'style_suffix', 'style_number', 'style', 'item_sku', 'sku']),
        style_name: findCol(['style_suffix', 'style_name', 'item_name', 'product_name', 'description']),
        commodity: findCol(['commodity', 'category', 'product_type', 'item_type']),
        color: findCol(['color', 'colour', 'item_color']),
        quantity: findCol(['quantity', 'qty', 'ordered_qty', 'units', 'quantity_ordered']),
        unit_price: findCol(['so_price_fcy', 'rate', 'unit_price', 'price', 'item_price']),
        total_amount: findCol(['total_fcy', 'total_amount', 'total', 'amount']),
        delivery_date: findCol(['so_cancel_date', 'delivery_date', 'ship_date', 'due_date']),
        status: findCol(['so_status', 'status', 'order_status']),
        image_url: findCol(['style_image', 'image_url', 'image', 'workdrive_link']),
        po_number: findCol(['purchase_order_number', 'po_number', 'po']),
        vendor_name: findCol(['vendor_name', 'vendor', 'supplier', 'factory']),
        po_status: findCol(['po_status', 'purchase_status']),
        po_quantity: findCol(['po_quantity', 'po_qty']),
        po_unit_price: findCol(['po_price_fcy', 'po_unit_price', 'po_rate']),
        po_total: findCol(['po_total_fcy', 'po_total', 'po_amount']),
        po_warehouse_date: findCol(['in_warehouse_date', 'po_warehouse_date', 'warehouse_date', 'eta'])
    };

    console.log('CSV Headers:', headers);
    console.log('Column mapping:', JSON.stringify(mapping));

    // Track SO+style+color combos to prevent double-counting SO quantities
    // The PO-SO Query CSV has one row per PO line, so the same SO quantity appears multiple times
    var seenSOKeys = {};

    for (var row of rows) {
        try {
            var getValue = function(field) {
                return field && row[field] ? row[field].replace(/^"|"$/g, '').trim() : '';
            };
            var getNumber = function(field) {
                var val = getValue(field).replace(/[,$]/g, '');
                return parseFloat(val) || 0;
            };
            var getDate = function(field) {
                var val = getValue(field);
                if (!val) return null;
                var d = new Date(val);
                return isNaN(d.getTime()) ? null : d.toISOString().split('T')[0];
            };

            var soNumber = getValue(mapping.so_number);
            var customer = getValue(mapping.customer);
            var styleNumber = getValue(mapping.style_number);

            if (!soNumber || !customer || !styleNumber) continue;

            var styleName = getValue(mapping.style_name) || styleNumber;
            var commodity = getValue(mapping.commodity) || '';
            var color = getValue(mapping.color) || '';
            var quantity = getNumber(mapping.quantity);
            var unitPrice = getNumber(mapping.unit_price);
            var totalAmount = getNumber(mapping.total_amount) || (quantity * unitPrice);
            var deliveryDate = getDate(mapping.delivery_date);
            var status = getValue(mapping.status) || 'Open';
            var imageUrl = getValue(mapping.image_url) || '';

            // Normalize status
            var statusLower = status.toLowerCase();
            if (statusLower.includes('partial')) status = 'Partial';
            else if (statusLower.includes('invoice') || statusLower.includes('shipped') || statusLower.includes('complete') || statusLower.includes('billed')) status = 'Invoiced';
            else status = 'Open';

            // PO data
            var poNumber = getValue(mapping.po_number);
            var vendorName = getValue(mapping.vendor_name);
            var poStatus = getValue(mapping.po_status);
            var poQuantity = getNumber(mapping.po_quantity);
            var poUnitPrice = getNumber(mapping.po_unit_price);
            var poTotal = getNumber(mapping.po_total);
            var poWarehouseDate = getDate(mapping.po_warehouse_date);

            // Dedup: only count SO quantity/total on first occurrence of SO+style+color
            // This prevents double-counting when the PO-SO join creates multiple rows per SO line
            var soKey = soNumber + '|' + styleNumber + '|' + color;
            if (seenSOKeys[soKey]) {
                // Duplicate SO line (different PO) - zero out SO quantities to avoid double-counting
                quantity = 0;
                unitPrice = 0;
                totalAmount = 0;
            } else {
                seenSOKeys[soKey] = true;
            }

            await pool.query(`
                INSERT INTO order_items (so_number, customer, style_number, style_name, commodity, color, quantity, unit_price, total_amount, delivery_date, status, image_url, po_number, vendor_name, po_status, po_quantity, po_unit_price, po_total, po_warehouse_date)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
            `, [soNumber, customer, styleNumber, styleName, commodity, color, quantity, unitPrice, totalAmount, deliveryDate, status, imageUrl, poNumber, vendorName, poStatus, poQuantity, poUnitPrice, poTotal, poWarehouseDate]);

            imported++;
        } catch (err) {
            errors.push('Row: ' + err.message);
            if (errors.length > 10) break;
        }
    }

    // Log import with filename in error_message field (for tracking)
    await pool.query('INSERT INTO import_history (import_type, status, records_imported, error_message) VALUES ($1, $2, $3, $4)',
        ['workdrive_sync', 'success', imported, fileName]);

    // Report freshness to admin panel
    await reportDataFreshness('Open Orders', imported, 'WorkDrive sync: ' + fileName);

    console.log('WorkDrive sync complete: ' + imported + ' records imported from ' + fileName);
    return { success: true, imported: imported, fileName: fileName, errors: errors };
}

// ============================================
// IMPORT PO SYNC FROM WORKDRIVE
// ============================================
// Separate sync function for Import PO data (different CSV file/folder)
async function syncImportPOsFromWorkDrive(force) {
    console.log('Starting Import PO sync from WorkDrive... (force=' + !!force + ')');

    if (!WORKDRIVE_IMPORT_PO_FOLDER_ID) {
        return { success: false, error: 'WorkDrive folder not configured. Set WORKDRIVE_SYNC_FOLDER_ID in environment.' };
    }

    // List files in folder
    var listResult = await listWorkDriveFolder(WORKDRIVE_IMPORT_PO_FOLDER_ID);
    if (!listResult.success) {
        return listResult;
    }

    // Find latest CSV
    var latestFile = findLatestCSV(listResult.files);
    if (!latestFile) {
        return { success: false, error: 'No CSV files found in Import PO folder' };
    }

    var fileName = latestFile.attributes.name;
    var fileId = latestFile.id;
    var modifiedTime = latestFile.attributes.modified_time;

    console.log('Found Import PO file: ' + fileName + ' (modified: ' + modifiedTime + ')');

    // Check if already imported (unless forced)
    if (!force) {
        var lastImport = await pool.query(
            "SELECT * FROM import_history WHERE import_type = 'import_po_sync' AND error_message = $1 ORDER BY created_at DESC LIMIT 1",
            [fileName]
        );
        if (lastImport.rows.length > 0) {
            var lastImportTime = new Date(lastImport.rows[0].created_at);
            var fileModTime = new Date(modifiedTime);
            var lastRecords = lastImport.rows[0].records_imported || 0;
            if (fileModTime <= lastImportTime && lastRecords > 0) {
                return { success: true, message: 'Import PO file already imported: ' + fileName, skipped: true };
            }
        }
    }

    // Download the file
    console.log('Downloading Import PO file: ' + fileName);
    var downloadResult = await downloadWorkDriveFile(fileId);
    if (!downloadResult.success) {
        return downloadResult;
    }

    // Parse CSV - Import PO specific columns
    var rows = downloadResult.content.split('\n');
    if (rows.length < 2) {
        return { success: false, error: 'CSV file is empty or has no data rows' };
    }

    var headers = rows[0].split(',').map(function(h) { return h.trim().toLowerCase().replace(/[^a-z0-9_]/g, '_'); });
    console.log('Import PO CSV headers:', headers);

    // Map column names (flexible naming)
    var findCol = function(names) {
        for (var i = 0; i < names.length; i++) {
            var idx = headers.indexOf(names[i].toLowerCase());
            if (idx >= 0) return idx;
        }
        return -1;
    };

    var mapping = {
        po_number: findCol(['purchase_order_number', 'po_number', 'po', 'po#']),
        vendor_name: findCol(['vendor_name', 'vendor', 'supplier', 'factory']),
        style_number: findCol(['style_name', 'style_suffix', 'style_number', 'style', 'sku', 'item_number', 'item']),
        style_name: findCol(['style_name', 'description', 'item_name', 'product_name']),
        commodity: findCol(['commodity', 'category', 'product_type', 'type']),
        color: findCol(['color', 'colour', 'variant']),
        po_quantity: findCol(['po_quantity', 'quantity', 'qty', 'order_qty']),
        po_unit_price: findCol(['po_price_fcy', 'po_unit_price', 'unit_price', 'price', 'rate', 'cost']),
        po_total: findCol(['po_total_fcy', 'po_total', 'total', 'amount', 'total_amount']),
        po_warehouse_date: findCol(['po_warehouse_date', 'warehouse_date', 'eta', 'delivery_date', 'expected_date']),
        po_status: findCol(['po_status', 'status', 'order_status']),
        image_url: findCol(['style_image', 'image_url', 'image', 'workdrive_link']),
        customer: findCol(['customer', 'customer_name', 'client'])
    };

    console.log('Import PO column mapping:', mapping);

    // Process rows
    var imported = 0;
    var updated = 0;
    var errors = [];

    // Clear existing Import PO data before loading new data (keep sales order data)
    var deleted = await pool.query("DELETE FROM order_items WHERE so_number LIKE 'IMP-%'");
    console.log('Cleared ' + (deleted.rowCount || 0) + ' existing Import PO rows');

    for (var i = 1; i < rows.length; i++) {
        if (!rows[i].trim()) continue;

        try {
            // Parse CSV row (handle quoted values)
            var values = [];
            var inQuotes = false;
            var current = '';
            for (var j = 0; j < rows[i].length; j++) {
                var char = rows[i][j];
                if (char === '"') {
                    inQuotes = !inQuotes;
                } else if (char === ',' && !inQuotes) {
                    values.push(current.trim());
                    current = '';
                } else {
                    current += char;
                }
            }
            values.push(current.trim());

            var getValue = function(idx) {
                if (idx < 0 || idx >= values.length) return '';
                return values[idx].replace(/^["']|["']$/g, '').trim();
            };

            var getNumber = function(idx) {
                var val = getValue(idx);
                if (!val) return 0;
                var num = parseFloat(val.replace(/[,$]/g, ''));
                return isNaN(num) ? 0 : num;
            };

            // Extract PO data
            var poNumber = getValue(mapping.po_number);
            var vendorName = getValue(mapping.vendor_name);
            var styleNumber = getValue(mapping.style_number);
            var styleName = getValue(mapping.style_name);
            var commodity = getValue(mapping.commodity);
            var color = getValue(mapping.color);
            var poQuantity = getNumber(mapping.po_quantity);
            var poUnitPrice = getNumber(mapping.po_unit_price);
            var poTotal = getNumber(mapping.po_total);
            var poWarehouseDate = getValue(mapping.po_warehouse_date);
            var poStatus = getValue(mapping.po_status) || 'Open';
            var imageUrl = getValue(mapping.image_url);
            var customer = getValue(mapping.customer);

            // Skip rows without required data
            if (!poNumber || !styleNumber) {
                continue;
            }

            // Calculate total if not provided
            if (!poTotal && poQuantity && poUnitPrice) {
                poTotal = poQuantity * poUnitPrice;
            }

            // Parse date
            var parsedDate = null;
            if (poWarehouseDate) {
                var d = new Date(poWarehouseDate);
                if (!isNaN(d.getTime())) {
                    parsedDate = d.toISOString().split('T')[0];
                }
            }

            // Upsert: check if PO+style+color exists
            var existingRow = await pool.query(
                'SELECT id FROM order_items WHERE po_number = $1 AND style_number = $2 AND (color = $3 OR (color IS NULL AND $3 IS NULL))',
                [poNumber, styleNumber, color || null]
            );

            if (existingRow.rows.length > 0) {
                // Update existing
                await pool.query(`
                    UPDATE order_items SET
                        vendor_name = COALESCE(NULLIF($1, ''), vendor_name),
                        style_name = COALESCE(NULLIF($2, ''), style_name),
                        commodity = COALESCE(NULLIF($3, ''), commodity),
                        po_quantity = $4,
                        po_unit_price = $5,
                        po_total = $6,
                        po_warehouse_date = COALESCE($7, po_warehouse_date),
                        po_status = COALESCE(NULLIF($8, ''), po_status),
                        image_url = COALESCE(NULLIF($9, ''), image_url),
                        customer = COALESCE(NULLIF($10, ''), customer)
                    WHERE po_number = $11 AND style_number = $12 AND (color = $13 OR (color IS NULL AND $13 IS NULL))
                `, [vendorName, styleName, commodity, poQuantity, poUnitPrice, poTotal, parsedDate, poStatus, imageUrl, customer, poNumber, styleNumber, color || null]);
                updated++;
            } else {
                // Insert new - use "IMP-" prefix for so_number since Import POs may not have a sales order
                var soNumber = 'IMP-' + poNumber;
                await pool.query(`
                    INSERT INTO order_items (so_number, po_number, vendor_name, style_number, style_name, commodity, color, po_quantity, po_unit_price, po_total, po_warehouse_date, po_status, image_url, customer, status)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'Open')
                `, [soNumber, poNumber, vendorName, styleNumber, styleName, commodity, color, poQuantity, poUnitPrice, poTotal, parsedDate, poStatus, imageUrl, customer]);
                imported++;
            }
        } catch (rowErr) {
            errors.push({ row: i, error: rowErr.message });
        }
    }

    // Log import
    await pool.query('INSERT INTO import_history (import_type, status, records_imported, error_message) VALUES ($1, $2, $3, $4)',
        ['import_po_sync', 'success', imported + updated, fileName]);

    console.log('Import PO sync complete: ' + imported + ' inserted, ' + updated + ' updated from ' + fileName);
    return { success: true, imported: imported, updated: updated, total: imported + updated, fileName: fileName, errors: errors };
}

// ============================================
// ZOHO IMAGE FETCHING - Simple direct approach
// ============================================

// Simple image fetch from Zoho - try standard API first, then CDN
async function fetchZohoImage(fileId) {
    if (!zohoAccessToken) {
        await refreshZohoToken();
    }

    // Try standard WorkDrive API first (more reliable with OAuth)
    var apiUrl = 'https://workdrive.zoho.com/api/v1/download/' + fileId;
    var cdnUrl = 'https://download-accl.zoho.com/v1/workdrive/download/' + fileId;

    var response = await fetch(apiUrl, {
        headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
    });

    var firstStatus = response.status;

    // If API fails, try CDN URL
    if (response.status === 401 || response.status === 404) {
        response = await fetch(cdnUrl, {
            headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
        });
    }

    var secondStatus = response.status;

    // If still 401, refresh token and retry both
    if (response.status === 401) {
        var refreshResult = await refreshZohoToken(true);
        if (!refreshResult.success) {
            throw new Error('Image fetch failed: 401 (token refresh failed: ' + refreshResult.error + ')');
        }
        response = await fetch(apiUrl, {
            headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
        });
        if (response.status === 401 || response.status === 404) {
            response = await fetch(cdnUrl, {
                headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
            });
        }
    }

    if (!response.ok) {
        throw new Error('Image fetch failed: ' + response.status + ' (api=' + firstStatus + ', cdn=' + secondStatus + ')');
    }

    var buffer = Buffer.from(await response.arrayBuffer());
    var contentType = response.headers.get('content-type') || 'image/jpeg';
    return { buffer, contentType };
}

async function initDB() {
    try {
        // Sales orders table - main order header info
        await pool.query(`CREATE TABLE IF NOT EXISTS sales_orders (
            id SERIAL PRIMARY KEY,
            so_number VARCHAR(100) UNIQUE NOT NULL,
            customer VARCHAR(255) NOT NULL,
            order_date DATE,
            delivery_date DATE,
            status VARCHAR(50) DEFAULT 'Open',
            total_amount DECIMAL(12,2) DEFAULT 0,
            total_qty INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Sales order line items - individual style/color/qty rows
        await pool.query(`CREATE TABLE IF NOT EXISTS order_items (
            id SERIAL PRIMARY KEY,
            so_number VARCHAR(100) NOT NULL,
            customer VARCHAR(255) NOT NULL,
            style_number VARCHAR(100) NOT NULL,
            style_name VARCHAR(255),
            commodity VARCHAR(100),
            color VARCHAR(100),
            quantity INTEGER DEFAULT 0,
            unit_price DECIMAL(10,2) DEFAULT 0,
            total_amount DECIMAL(12,2) DEFAULT 0,
            delivery_date DATE,
            status VARCHAR(50) DEFAULT 'Open',
            image_url TEXT,
            po_number VARCHAR(100),
            vendor_name VARCHAR(255),
            po_status VARCHAR(50),
            po_quantity INTEGER DEFAULT 0,
            po_unit_price DECIMAL(10,2) DEFAULT 0,
            po_total DECIMAL(12,2) DEFAULT 0,
            po_warehouse_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Indexes for fast filtering
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_customer ON order_items(customer)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_delivery ON order_items(delivery_date)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_commodity ON order_items(commodity)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_status ON order_items(status)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_style ON order_items(style_number)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_so ON order_items(so_number)');
        // Add missing columns if they don't exist (migrations for PO fields)
        await pool.query(`ALTER TABLE order_items ADD COLUMN IF NOT EXISTS po_number VARCHAR(100)`);
        await pool.query(`ALTER TABLE order_items ADD COLUMN IF NOT EXISTS vendor_name VARCHAR(255)`);
        await pool.query(`ALTER TABLE order_items ADD COLUMN IF NOT EXISTS po_status VARCHAR(50)`);
        await pool.query(`ALTER TABLE order_items ADD COLUMN IF NOT EXISTS po_quantity INTEGER DEFAULT 0`);
        await pool.query(`ALTER TABLE order_items ADD COLUMN IF NOT EXISTS po_unit_price DECIMAL(10,2) DEFAULT 0`);
        await pool.query(`ALTER TABLE order_items ADD COLUMN IF NOT EXISTS po_total DECIMAL(12,2) DEFAULT 0`);
        await pool.query(`ALTER TABLE order_items ADD COLUMN IF NOT EXISTS po_warehouse_date DATE`);
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_vendor ON order_items(vendor_name)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_po ON order_items(po_number)');
        await pool.query('CREATE INDEX IF NOT EXISTS idx_order_items_po_status ON order_items(po_status)');

        // Zoho tokens table (shared pattern)
        await pool.query(`CREATE TABLE IF NOT EXISTS zoho_tokens (
            id SERIAL PRIMARY KEY,
            access_token TEXT,
            refresh_token TEXT,
            expires_at TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Import history
        await pool.query(`CREATE TABLE IF NOT EXISTS import_history (
            id SERIAL PRIMARY KEY,
            import_type VARCHAR(50),
            status VARCHAR(50),
            records_imported INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Image cache table - store images in database for persistence
        await pool.query(`CREATE TABLE IF NOT EXISTS image_cache (
            id SERIAL PRIMARY KEY,
            file_id VARCHAR(100) UNIQUE NOT NULL,
            image_data BYTEA NOT NULL,
            content_type VARCHAR(50) DEFAULT 'image/jpeg',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);
        await pool.query('CREATE INDEX IF NOT EXISTS idx_image_cache_file_id ON image_cache(file_id)');

        // App settings table
        await pool.query(`CREATE TABLE IF NOT EXISTS app_settings (
            id SERIAL PRIMARY KEY,
            setting_key VARCHAR(100) UNIQUE NOT NULL,
            setting_value TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )`);

        // Export jobs table - track Zoho Flow export triggers and callbacks
        await pool.query(`CREATE TABLE IF NOT EXISTS export_jobs (
            id SERIAL PRIMARY KEY,
            job_id VARCHAR(100) UNIQUE NOT NULL,
            job_type VARCHAR(50) DEFAULT 'import_po',
            status VARCHAR(50) DEFAULT 'pending',
            triggered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            completed_at TIMESTAMP,
            error_message TEXT
        )`);

        // Load stored Zoho token
        var tokenResult = await pool.query('SELECT * FROM zoho_tokens ORDER BY id DESC LIMIT 1');
        if (tokenResult.rows.length > 0) {
            zohoAccessToken = tokenResult.rows[0].access_token;
            console.log('Loaded stored Zoho access token');
        }

        console.log('Database initialized successfully');
    } catch (err) {
        console.error('Database initialization error:', err);
    }
}

// Zoho token refresh
var lastTokenRefreshAttempt = 0;
var tokenRefreshCooldown = 30000; // 30 second cooldown between refresh attempts
var lastTokenRefreshError = null;

async function refreshZohoToken(forceRefresh) {
    // Prevent hammering the token endpoint (unless force refresh)
    var now = Date.now();
    if (!forceRefresh && now - lastTokenRefreshAttempt < tokenRefreshCooldown) {
        console.log('Token refresh on cooldown, skipping (last error: ' + lastTokenRefreshError + ')');
        return { success: false, error: lastTokenRefreshError || 'On cooldown' };
    }
    lastTokenRefreshAttempt = now;

    try {
        var clientId = process.env.ZOHO_CLIENT_ID;
        var clientSecret = process.env.ZOHO_CLIENT_SECRET;
        var refreshToken = process.env.ZOHO_REFRESH_TOKEN;
        if (!clientId || !clientSecret || !refreshToken) {
            lastTokenRefreshError = 'Zoho credentials not configured';
            console.log('Token refresh failed: credentials not configured');
            return { success: false, error: lastTokenRefreshError };
        }
        console.log('Attempting Zoho token refresh...');
        var params = new URLSearchParams();
        params.append('refresh_token', refreshToken);
        params.append('client_id', clientId);
        params.append('client_secret', clientSecret);
        params.append('grant_type', 'refresh_token');
        var response = await fetch('https://accounts.zoho.com/oauth/v2/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params.toString()
        });
        var data = await response.json();
        if (data.access_token) {
            zohoAccessToken = data.access_token;
            lastTokenRefreshError = null;
            var expiresAt = new Date(Date.now() + (data.expires_in * 1000));
            await pool.query('INSERT INTO zoho_tokens (access_token, refresh_token, expires_at, updated_at) VALUES ($1, $2, $3, NOW())',
                [zohoAccessToken, refreshToken, expiresAt]);
            console.log('Zoho token refreshed successfully, expires:', expiresAt);
            return { success: true };
        }
        lastTokenRefreshError = data.error || 'Token refresh failed: ' + JSON.stringify(data);
        console.log('Token refresh failed:', lastTokenRefreshError);
        return { success: false, error: lastTokenRefreshError };
    } catch (err) {
        lastTokenRefreshError = err.message;
        console.log('Token refresh exception:', err.message);
        return { success: false, error: err.message };
    }
}

// ============================================
// API ENDPOINTS
// ============================================

// Get all order items with filters
app.get('/api/orders', async function(req, res) {
    try {
        var customer = req.query.customer;
        var customers = req.query.customers; // comma-separated list for multi-select
        var commodity = req.query.commodity;
        var commodities = req.query.commodities; // comma-separated list for multi-select
        var month = req.query.month; // Format: YYYY-MM
        var months = req.query.months; // comma-separated list for multi-select
        var year = req.query.year; // Calendar year filter
        var years = req.query.years; // comma-separated list for multi-select
        var fiscalYear = req.query.fiscalYear; // Fiscal year filter (June 1 - May 31)
        var fiscalYears = req.query.fiscalYears; // comma-separated list for multi-select
        var status = req.query.status || 'Open';
        var style = req.query.style;
        var soNumber = req.query.so_number;

        var conditions = [];
        var params = [];
        var paramIndex = 1;

        // Check for includeFiscalYears setting to exclude old data
        var settingsResult = await pool.query("SELECT setting_value FROM app_settings WHERE setting_key = 'includeFiscalYears'");
        if (settingsResult.rows.length > 0 && settingsResult.rows[0].setting_value) {
            var includedFYs = settingsResult.rows[0].setting_value.split(',').map(fy => parseInt(fy.trim())).filter(fy => !isNaN(fy));
            if (includedFYs.length > 0) {
                // Build OR conditions for each included FY date range
                var fyConditions = includedFYs.map(function(fy) {
                    return "(delivery_date::date >= '" + (fy - 1) + "-06-01'::date AND delivery_date::date <= '" + fy + "-05-31'::date)";
                });
                conditions.push('(' + fyConditions.join(' OR ') + ')');
                console.log('Included FYs filter:', includedFYs.join(', '));
            }
        }

        if (status && status !== 'All') {
            // "Open" includes both Open and Partial statuses
            if (status === 'Open') {
                conditions.push("status IN ('Open', 'Partial')");
            } else {
                conditions.push('status = $' + paramIndex++);
                params.push(status);
            }
        }
        if (customer) {
            conditions.push('customer = $' + paramIndex++);
            params.push(customer);
        }
        if (customers) {
            var customerList = customers.split('||').map(c => c.trim()).filter(c => c);
            if (customerList.length > 0) {
                conditions.push('customer = ANY($' + paramIndex++ + ')');
                params.push(customerList);
            }
        }
        if (commodity) {
            conditions.push('commodity = $' + paramIndex++);
            params.push(commodity);
        }
        // Multi-select commodities filter
        if (commodities) {
            var commodityList = commodities.split(',').map(c => c.trim()).filter(c => c);
            if (commodityList.length > 0) {
                conditions.push('commodity = ANY($' + paramIndex++ + ')');
                params.push(commodityList);
            }
        }
        if (year) {
            conditions.push("EXTRACT(YEAR FROM delivery_date) = $" + paramIndex++);
            params.push(parseInt(year));
        }
        // Multi-select years filter
        if (years) {
            var yearList = years.split(',').map(y => parseInt(y.trim())).filter(y => !isNaN(y));
            if (yearList.length > 0) {
                conditions.push("EXTRACT(YEAR FROM delivery_date) = ANY($" + paramIndex++ + ")");
                params.push(yearList);
            }
        }
        if (fiscalYear) {
            // Fiscal year runs June 1 to May 31
            // FY2026 = June 1, 2025 - May 31, 2026
            var fyInt = parseInt(fiscalYear);
            var fyStart = (fyInt - 1) + '-06-01';
            var fyEnd = fyInt + '-05-31';
            conditions.push("delivery_date::date >= $" + paramIndex + "::date AND delivery_date::date <= $" + (paramIndex + 1) + "::date");
            params.push(fyStart);
            params.push(fyEnd);
            paramIndex += 2;
            console.log('FY Filter applied: FY' + fiscalYear + ' = ' + fyStart + ' to ' + fyEnd);
        }
        // Multi-select fiscal years filter
        if (fiscalYears) {
            var fyList = fiscalYears.split(',').map(fy => parseInt(fy.trim())).filter(fy => !isNaN(fy));
            if (fyList.length > 0) {
                // Build OR conditions for each fiscal year range
                var fyConditions = fyList.map(function(fy) {
                    return "(delivery_date::date >= '" + (fy - 1) + "-06-01'::date AND delivery_date::date <= '" + fy + "-05-31'::date)";
                });
                conditions.push('(' + fyConditions.join(' OR ') + ')');
                console.log('Multi-FY Filter applied:', fyList.join(', '));
            }
        }
        if (month) {
            conditions.push("TO_CHAR(delivery_date, 'YYYY-MM') = $" + paramIndex++);
            params.push(month);
        }
        // Multi-select months filter
        if (months) {
            var monthList = months.split(',').map(m => m.trim()).filter(m => m);
            if (monthList.length > 0) {
                conditions.push("TO_CHAR(delivery_date, 'YYYY-MM') = ANY($" + paramIndex++ + ")");
                params.push(monthList);
            }
        }
        if (style) {
            conditions.push('style_number = $' + paramIndex++);
            params.push(style);
        }
        // Department filter - department is the last letter of the base style (before the dash suffix)
        var departments = req.query.departments;
        if (departments) {
            var deptList = departments.split(',').map(d => d.trim()).filter(d => d);
            if (deptList.length > 0) {
                var deptConditions = deptList.map(function(d) {
                    return "UPPER(SUBSTRING(style_number FROM '^[0-9]+([A-Za-z])')) = '" + d.toUpperCase().replace(/'/g, '') + "'";
                });
                conditions.push('(' + deptConditions.join(' OR ') + ')');
            }
        }
        var styleSearch = req.query.styleSearch;
        if (styleSearch) {
            conditions.push('style_number ILIKE $' + paramIndex++);
            params.push('%' + styleSearch + '%');
        }
        if (soNumber) {
            conditions.push('so_number = $' + paramIndex++);
            params.push(soNumber);
        }

        var whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

        // Debug logging
        console.log('=== /api/orders DEBUG ===');
        console.log('Fiscal Year param received:', fiscalYear || 'NONE');
        console.log('Status param received:', status);
        console.log('Where clause:', whereClause);
        console.log('Params:', JSON.stringify(params));

        // Extra check: if FY2026 selected, verify no May 2025 data
        if (fiscalYear === '2026') {
            var mayCheck = await pool.query(
                "SELECT COUNT(*) as may_count FROM order_items WHERE " +
                conditions.join(' AND ') +
                " AND TO_CHAR(delivery_date, 'YYYY-MM') = '2025-05'",
                params
            );
            console.log('MAY 2025 count with FY2026 filter:', mayCheck.rows[0].may_count);
        }

        var query = `
            SELECT
                style_number,
                style_name,
                commodity,
                image_url,
                json_agg(json_build_object(
                    'id', id,
                    'so_number', so_number,
                    'customer', customer,
                    'color', color,
                    'quantity', quantity,
                    'unit_price', unit_price,
                    'total_amount', total_amount,
                    'delivery_date', delivery_date,
                    'status', status,
                    'po_unit_price', po_unit_price,
                    'po_total', po_total,
                    'po_quantity', po_quantity
                ) ORDER BY delivery_date, so_number) as orders,
                SUM(quantity) as total_qty,
                SUM(total_amount) as total_dollars,
                COUNT(DISTINCT so_number) as order_count
            FROM order_items
            ${whereClause}
            GROUP BY style_number, style_name, commodity, image_url
            ORDER BY SUM(total_amount) DESC
            LIMIT 500
        `;

        var result = await pool.query(query, params);

        // Get summary stats
        var statsQuery = `
            SELECT
                COUNT(DISTINCT style_number) as style_count,
                COUNT(DISTINCT so_number) as order_count,
                COUNT(DISTINCT customer) as customer_count,
                SUM(quantity) as total_qty,
                SUM(total_amount) as total_dollars
            FROM order_items
            ${whereClause}
        `;
        var statsResult = await pool.query(statsQuery, params);

        // Get full commodity breakdown (no limit)
        var commodityQuery = `
            SELECT commodity, SUM(total_amount) as total_dollars, SUM(quantity) as total_qty
            FROM order_items
            ${whereClause}
            GROUP BY commodity
            ORDER BY SUM(total_amount) DESC
        `;
        var commodityResult = await pool.query(commodityQuery, params);

        // Get color breakdown (top 30 colors)
        var colorQuery = `
            SELECT color, SUM(total_amount) as total_dollars, SUM(quantity) as total_qty, COUNT(DISTINCT style_number) as style_count
            FROM order_items
            ${whereClause}
            AND color IS NOT NULL AND color != ''
            GROUP BY color
            ORDER BY SUM(total_amount) DESC
            LIMIT 30
        `;
        var colorResult = await pool.query(colorQuery, params);

        // Get full customer breakdown (no limit)
        var customerQuery = `
            SELECT customer, SUM(total_amount) as total_dollars, SUM(quantity) as total_qty
            FROM order_items
            ${whereClause}
            GROUP BY customer
            ORDER BY SUM(total_amount) DESC
        `;
        var customerResult = await pool.query(customerQuery, params);

        // Get full monthly breakdown (no limit)
        var monthlyQuery = `
            SELECT TO_CHAR(delivery_date, 'YYYY-MM') as month, SUM(total_amount) as total_dollars, SUM(quantity) as total_qty
            FROM order_items
            ${whereClause}
            GROUP BY TO_CHAR(delivery_date, 'YYYY-MM')
            ORDER BY TO_CHAR(delivery_date, 'YYYY-MM')
        `;
        var monthlyResult = await pool.query(monthlyQuery, params);

        // Get monthly breakdown BY commodity (for stacked bar chart)
        var monthlyByCommodityQuery = `
            SELECT TO_CHAR(delivery_date, 'YYYY-MM') as month, COALESCE(commodity, 'Other') as commodity, SUM(total_amount) as total_dollars
            FROM order_items
            ${whereClause}
            GROUP BY TO_CHAR(delivery_date, 'YYYY-MM'), commodity
            ORDER BY TO_CHAR(delivery_date, 'YYYY-MM'), total_dollars DESC
        `;
        var monthlyByCommodityResult = await pool.query(monthlyByCommodityQuery, params);

        // YoY Comparison: Get previous fiscal year data if a fiscal year is selected
        var prevYearCommodity = [];
        var prevYearCustomer = [];
        var missingCustomers = [];

        if (fiscalYear) {
            var prevFY = parseInt(fiscalYear) - 1;
            var prevFYStart = (prevFY - 1) + '-06-01';
            var prevFYEnd = prevFY + '-05-31';

            // Previous year commodity breakdown
            var prevCommodityQuery = `
                SELECT commodity, SUM(total_amount) as total_dollars
                FROM order_items
                WHERE delivery_date::date >= '${prevFYStart}'::date
                AND delivery_date::date <= '${prevFYEnd}'::date
                AND status IN ('Open', 'Partial', 'Invoiced')
                GROUP BY commodity
            `;
            var prevCommodityResult = await pool.query(prevCommodityQuery);
            prevYearCommodity = prevCommodityResult.rows;

            // Previous year customer breakdown
            var prevCustomerQuery = `
                SELECT customer, SUM(total_amount) as total_dollars
                FROM order_items
                WHERE delivery_date::date >= '${prevFYStart}'::date
                AND delivery_date::date <= '${prevFYEnd}'::date
                AND status IN ('Open', 'Partial', 'Invoiced')
                GROUP BY customer
            `;
            var prevCustomerResult = await pool.query(prevCustomerQuery);
            prevYearCustomer = prevCustomerResult.rows;

            // Missing customers: bought last year but not this year
            var fyStart = (parseInt(fiscalYear) - 1) + '-06-01';
            var fyEnd = fiscalYear + '-05-31';
            var missingQuery = `
                SELECT py.customer, py.total_dollars as prev_year_dollars, py.last_order
                FROM (
                    SELECT customer, SUM(total_amount) as total_dollars, MAX(delivery_date) as last_order
                    FROM order_items
                    WHERE delivery_date::date >= '${prevFYStart}'::date
                    AND delivery_date::date <= '${prevFYEnd}'::date
                    GROUP BY customer
                ) py
                LEFT JOIN (
                    SELECT DISTINCT customer
                    FROM order_items
                    WHERE delivery_date::date >= '${fyStart}'::date
                    AND delivery_date::date <= '${fyEnd}'::date
                ) cy ON py.customer = cy.customer
                WHERE cy.customer IS NULL
                ORDER BY py.total_dollars DESC
                LIMIT 10
            `;
            var missingResult = await pool.query(missingQuery);
            missingCustomers = missingResult.rows;
        }

        // Prevent browser caching of filtered results
        res.set('Cache-Control', 'no-store, no-cache, must-revalidate');
        res.set('Pragma', 'no-cache');
        res.json({
            items: result.rows,
            stats: statsResult.rows[0],
            commodityBreakdown: commodityResult.rows,
            colorBreakdown: colorResult.rows,
            customerBreakdown: customerResult.rows,
            monthlyBreakdown: monthlyResult.rows,
            monthlyByCommodity: monthlyByCommodityResult.rows,
            prevYearCommodity: prevYearCommodity,
            prevYearCustomer: prevYearCustomer,
            missingCustomers: missingCustomers,
            fiscalYear: fiscalYear
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get orders grouped by Sales Order number
app.get('/api/orders/by-so', async function(req, res) {
    try {
        var customer = req.query.customer;
        var customers = req.query.customers;
        var commodity = req.query.commodity;
        var commodities = req.query.commodities;
        var month = req.query.month;
        var months = req.query.months;
        var year = req.query.year;
        var years = req.query.years;
        var fiscalYear = req.query.fiscalYear;
        var fiscalYears = req.query.fiscalYears;
        var status = req.query.status || 'Open';

        var conditions = [];
        var params = [];
        var paramIndex = 1;

        // Check for includeFiscalYears setting to exclude old data
        var settingsResult = await pool.query("SELECT setting_value FROM app_settings WHERE setting_key = 'includeFiscalYears'");
        if (settingsResult.rows.length > 0 && settingsResult.rows[0].setting_value) {
            var includedFYs = settingsResult.rows[0].setting_value.split(',').map(fy => parseInt(fy.trim())).filter(fy => !isNaN(fy));
            if (includedFYs.length > 0) {
                var fyConditions = includedFYs.map(function(fy) {
                    return "(delivery_date::date >= '" + (fy - 1) + "-06-01'::date AND delivery_date::date <= '" + fy + "-05-31'::date)";
                });
                conditions.push('(' + fyConditions.join(' OR ') + ')');
            }
        }

        if (status && status !== 'All') {
            if (status === 'Open') {
                conditions.push("status IN ('Open', 'Partial')");
            } else {
                conditions.push('status = $' + paramIndex++);
                params.push(status);
            }
        }
        if (customer) {
            conditions.push('customer = $' + paramIndex++);
            params.push(customer);
        }
        if (customers) {
            var customerList = customers.split('||').map(c => c.trim()).filter(c => c);
            if (customerList.length > 0) {
                conditions.push('customer = ANY($' + paramIndex++ + ')');
                params.push(customerList);
            }
        }
        if (commodity) {
            conditions.push('commodity = $' + paramIndex++);
            params.push(commodity);
        }
        // Multi-select commodities filter
        if (commodities) {
            var commodityList = commodities.split(',').map(c => c.trim()).filter(c => c);
            if (commodityList.length > 0) {
                conditions.push('commodity = ANY($' + paramIndex++ + ')');
                params.push(commodityList);
            }
        }
        if (year) {
            conditions.push("EXTRACT(YEAR FROM delivery_date) = $" + paramIndex++);
            params.push(parseInt(year));
        }
        // Multi-select years filter
        if (years) {
            var yearList = years.split(',').map(y => parseInt(y.trim())).filter(y => !isNaN(y));
            if (yearList.length > 0) {
                conditions.push("EXTRACT(YEAR FROM delivery_date) = ANY($" + paramIndex++ + ")");
                params.push(yearList);
            }
        }
        if (fiscalYear) {
            var fyInt = parseInt(fiscalYear);
            var fyStart = (fyInt - 1) + '-06-01';
            var fyEnd = fyInt + '-05-31';
            conditions.push("delivery_date::date >= $" + paramIndex + "::date AND delivery_date::date <= $" + (paramIndex + 1) + "::date");
            params.push(fyStart);
            params.push(fyEnd);
            paramIndex += 2;
        }
        // Multi-select fiscal years filter
        if (fiscalYears) {
            var fyList = fiscalYears.split(',').map(fy => parseInt(fy.trim())).filter(fy => !isNaN(fy));
            if (fyList.length > 0) {
                var fyConditions = fyList.map(function(fy) {
                    return "(delivery_date::date >= '" + (fy - 1) + "-06-01'::date AND delivery_date::date <= '" + fy + "-05-31'::date)";
                });
                conditions.push('(' + fyConditions.join(' OR ') + ')');
            }
        }
        if (month) {
            conditions.push("TO_CHAR(delivery_date, 'YYYY-MM') = $" + paramIndex++);
            params.push(month);
        }
        // Multi-select months filter
        if (months) {
            var monthList = months.split(',').map(m => m.trim()).filter(m => m);
            if (monthList.length > 0) {
                conditions.push("TO_CHAR(delivery_date, 'YYYY-MM') = ANY($" + paramIndex++ + ")");
                params.push(monthList);
            }
        }

        var whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

        var query = `
            SELECT
                so_number,
                customer,
                MIN(delivery_date) as delivery_date,
                json_agg(json_build_object(
                    'id', id,
                    'style_number', style_number,
                    'style_name', style_name,
                    'commodity', commodity,
                    'color', color,
                    'quantity', quantity,
                    'unit_price', unit_price,
                    'total_amount', total_amount,
                    'image_url', image_url
                ) ORDER BY style_number, color) as items,
                SUM(quantity) as total_qty,
                SUM(total_amount) as total_dollars,
                COUNT(*) as line_count
            FROM order_items
            ${whereClause}
            GROUP BY so_number, customer
            ORDER BY MIN(delivery_date), so_number
            LIMIT 500
        `;

        var result = await pool.query(query, params);
        res.json({ orders: result.rows });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get filter options (customers, commodities, months, years)
app.get('/api/filters', async function(req, res) {
    try {
        var customersResult = await pool.query(`
            SELECT customer, COUNT(DISTINCT so_number) as order_count, SUM(total_amount) as total_dollars
            FROM order_items
            WHERE status = 'Open'
            GROUP BY customer
            ORDER BY customer
        `);

        var commoditiesResult = await pool.query(`
            SELECT commodity, COUNT(DISTINCT style_number) as style_count
            FROM order_items
            WHERE status = 'Open' AND commodity IS NOT NULL AND commodity != ''
            GROUP BY commodity
            ORDER BY commodity
        `);

        var monthsResult = await pool.query(`
            SELECT DISTINCT TO_CHAR(delivery_date, 'YYYY-MM') as month,
                   TO_CHAR(delivery_date, 'Mon YYYY') as display_name,
                   MIN(delivery_date) as sort_date
            FROM order_items
            WHERE status = 'Open' AND delivery_date IS NOT NULL
            GROUP BY TO_CHAR(delivery_date, 'YYYY-MM'), TO_CHAR(delivery_date, 'Mon YYYY')
            ORDER BY MIN(delivery_date)
        `);

        var yearsResult = await pool.query(`
            SELECT DISTINCT EXTRACT(YEAR FROM delivery_date)::INTEGER as year
            FROM order_items
            WHERE delivery_date IS NOT NULL
            ORDER BY year DESC
        `);

        // Calculate fiscal years (June 1 - May 31)
        // If month >= 6, fiscal year = calendar year + 1
        // If month < 6, fiscal year = calendar year
        var fiscalYearsResult = await pool.query(`
            SELECT DISTINCT
                CASE
                    WHEN EXTRACT(MONTH FROM delivery_date) >= 6
                    THEN EXTRACT(YEAR FROM delivery_date)::INTEGER + 1
                    ELSE EXTRACT(YEAR FROM delivery_date)::INTEGER
                END as fiscal_year
            FROM order_items
            WHERE delivery_date IS NOT NULL
            ORDER BY fiscal_year DESC
        `);

        // Filter fiscal years based on includeFiscalYears setting
        var allFiscalYears = fiscalYearsResult.rows.map(r => r.fiscal_year);
        var filteredFiscalYears = allFiscalYears;

        var settingsResult = await pool.query("SELECT setting_value FROM app_settings WHERE setting_key = 'includeFiscalYears'");
        if (settingsResult.rows.length > 0 && settingsResult.rows[0].setting_value) {
            var includedFYs = settingsResult.rows[0].setting_value.split(',').map(fy => parseInt(fy.trim())).filter(fy => !isNaN(fy));
            if (includedFYs.length > 0) {
                filteredFiscalYears = allFiscalYears.filter(fy => includedFYs.includes(fy));
            }
        }

        // Department counts - extract letter before dash from style_number (e.g., 85887J-AC -> J)
        var departmentsResult = await pool.query(`
            SELECT dept_code, COUNT(DISTINCT base_style) as style_count FROM (
                SELECT SUBSTRING(style_number FROM '^[0-9]+([A-Za-z])') as dept_code,
                       SUBSTRING(style_number FROM '^[0-9]+[A-Za-z]') as base_style
                FROM order_items
                WHERE style_number ~ '^[0-9]+[A-Za-z]'
            ) sub
            WHERE dept_code IS NOT NULL
            GROUP BY dept_code
            ORDER BY style_count DESC
        `);

        res.json({
            customers: customersResult.rows,
            commodities: commoditiesResult.rows,
            departments: departmentsResult.rows,
            months: monthsResult.rows,
            years: yearsResult.rows.map(r => r.year),
            fiscalYears: filteredFiscalYears,
            allFiscalYears: allFiscalYears // For settings modal
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get PO filters (vendors, commodities for PO view)
app.get('/api/po/filters', async function(req, res) {
    try {
        var statusFilter = req.query.status || 'Open';
        var statusCondition = statusFilter === 'All' ? '1=1' : 'po_status = $1';
        var statusParams = statusFilter === 'All' ? [] : [statusFilter];

        var vendorsResult = await pool.query(`
            WITH unique_pos AS (
                SELECT DISTINCT ON (po_number, style_number)
                    po_number, style_number, vendor_name, commodity, po_total
                FROM order_items
                WHERE vendor_name IS NOT NULL AND vendor_name != '' AND po_number IS NOT NULL AND po_number != '' AND ${statusCondition}
                ORDER BY po_number, style_number, id
            )
            SELECT vendor_name, COUNT(DISTINCT po_number) as po_count, SUM(po_total) as total_dollars
            FROM unique_pos
            GROUP BY vendor_name
            ORDER BY vendor_name
        `, statusParams);

        var commoditiesResult = await pool.query(`
            WITH unique_pos AS (
                SELECT DISTINCT ON (po_number, style_number)
                    po_number, style_number, commodity
                FROM order_items
                WHERE ${statusCondition} AND commodity IS NOT NULL AND commodity != '' AND po_number IS NOT NULL AND po_number != ''
                ORDER BY po_number, style_number, id
            )
            SELECT commodity, COUNT(DISTINCT style_number) as style_count
            FROM unique_pos
            GROUP BY commodity
            ORDER BY commodity
        `, statusParams);

        res.json({
            vendors: vendorsResult.rows,
            commodities: commoditiesResult.rows
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get PO data (grouped by style, like sales orders)
// Uses DISTINCT ON to deduplicate PO lines that appear on multiple sales orders
app.get('/api/po/orders', async function(req, res) {
    try {
        var conditions = ['po_number IS NOT NULL', "po_number != ''"];
        var params = [];
        var paramIndex = 1;

        // Vendor filter (use || separator to avoid issues with commas in vendor names)
        if (req.query.vendors) {
            var vendors = req.query.vendors.split('||');
            conditions.push('vendor_name IN (' + vendors.map(() => '$' + paramIndex++).join(',') + ')');
            params.push(...vendors);
        }

        // Commodity filter
        if (req.query.commodity) {
            conditions.push('commodity = $' + paramIndex++);
            params.push(req.query.commodity);
        }
        if (req.query.commodities) {
            var commodityList = req.query.commodities.split(',').map(c => c.trim()).filter(c => c);
            if (commodityList.length > 0) {
                conditions.push('commodity = ANY($' + paramIndex++ + ')');
                params.push(commodityList);
            }
        }

        // PO Status filter
        if (req.query.status && req.query.status !== 'All') {
            conditions.push('po_status = $' + paramIndex++);
            params.push(req.query.status);
        }

        // Month filter (by warehouse date)
        if (req.query.months) {
            var monthList = req.query.months.split(',').map(m => m.trim()).filter(m => m);
            if (monthList.length > 0) {
                conditions.push("TO_CHAR(po_warehouse_date, 'YYYY-MM') = ANY($" + paramIndex++ + ")");
                params.push(monthList);
            }
        }

        // Style search (partial match)
        if (req.query.styleSearch) {
            conditions.push('style_number ILIKE $' + paramIndex++);
            params.push('%' + req.query.styleSearch + '%');
        }

        // Department filter
        var departments = req.query.departments;
        if (departments) {
            var deptList = departments.split(',').map(d => d.trim()).filter(d => d);
            if (deptList.length > 0) {
                var deptConditions = deptList.map(function(d) {
                    return "UPPER(SUBSTRING(style_number FROM '^[0-9]+([A-Za-z])')) = '" + d.toUpperCase().replace(/'/g, '') + "'";
                });
                conditions.push('(' + deptConditions.join(' OR ') + ')');
            }
        }

        var whereClause = 'WHERE ' + conditions.join(' AND ');

        // CTE to deduplicate PO lines - same PO can appear on multiple sales orders
        // Unique PO line = po_number + style_number + color
        var deduplicatedCTE = `
            WITH unique_pos AS (
                SELECT DISTINCT ON (po_number, style_number)
                    id, po_number, style_number, style_name, commodity, image_url, color,
                    vendor_name, po_quantity, po_unit_price, po_total, po_status, po_warehouse_date, unit_price
                FROM order_items
                ${whereClause}
                ORDER BY po_number, style_number, id
            )
        `;

        // Get items grouped by style with PO details as JSON array
        var query = `
            ${deduplicatedCTE}
            SELECT
                style_number,
                style_name,
                commodity,
                image_url,
                json_agg(json_build_object(
                    'id', id,
                    'po_number', po_number,
                    'vendor_name', vendor_name,
                    'color', color,
                    'po_quantity', po_quantity,
                    'po_unit_price', po_unit_price,
                    'po_total', po_total,
                    'po_status', po_status,
                    'po_warehouse_date', po_warehouse_date,
                    'unit_price', unit_price
                ) ORDER BY po_warehouse_date, po_number) as pos,
                SUM(po_quantity) as total_qty,
                SUM(po_total) as total_dollars,
                COUNT(DISTINCT po_number) as po_count
            FROM unique_pos
            GROUP BY style_number, style_name, commodity, image_url
            ORDER BY SUM(po_total) DESC
            LIMIT 500
        `;
        var itemsResult = await pool.query(query, params);

        // Get stats (deduplicated)
        var statsResult = await pool.query(`
            ${deduplicatedCTE}
            SELECT
                COUNT(DISTINCT po_number) as po_count,
                COUNT(DISTINCT vendor_name) as vendor_count,
                COUNT(DISTINCT style_number) as style_count,
                COALESCE(SUM(po_quantity), 0) as total_qty,
                COALESCE(SUM(po_total), 0) as total_dollars
            FROM unique_pos
        `, params);

        // Get vendor breakdown (deduplicated)
        var vendorResult = await pool.query(`
            ${deduplicatedCTE}
            SELECT vendor_name, SUM(po_total) as total_dollars, SUM(po_quantity) as total_qty
            FROM unique_pos
            GROUP BY vendor_name
            ORDER BY SUM(po_total) DESC
        `, params);

        // Get commodity breakdown (deduplicated)
        var commodityResult = await pool.query(`
            ${deduplicatedCTE}
            SELECT commodity, SUM(po_total) as total_dollars, SUM(po_quantity) as total_qty
            FROM unique_pos
            GROUP BY commodity
            ORDER BY SUM(po_total) DESC
        `, params);

        // Get monthly breakdown by warehouse date (deduplicated)
        var monthlyResult = await pool.query(`
            ${deduplicatedCTE}
            SELECT TO_CHAR(po_warehouse_date, 'YYYY-MM') as month, SUM(po_total) as total_dollars, SUM(po_quantity) as total_qty
            FROM unique_pos
            WHERE po_warehouse_date IS NOT NULL
            GROUP BY TO_CHAR(po_warehouse_date, 'YYYY-MM')
            ORDER BY TO_CHAR(po_warehouse_date, 'YYYY-MM')
        `, params);

        // Get color breakdown (deduplicated)
        var colorResult = await pool.query(`
            ${deduplicatedCTE}
            SELECT color, SUM(po_total) as total_dollars, SUM(po_quantity) as total_qty, COUNT(DISTINCT style_number) as style_count
            FROM unique_pos
            WHERE color IS NOT NULL AND color != ''
            GROUP BY color
            ORDER BY SUM(po_total) DESC
            LIMIT 30
        `, params);

        // Get monthly by commodity breakdown (for stacked bar chart)
        var monthlyByCommodityResult = await pool.query(`
            SELECT TO_CHAR(po_warehouse_date, 'YYYY-MM') as month, commodity, SUM(po_total) as total_dollars
            FROM order_items
            ${whereClause}
            AND po_warehouse_date IS NOT NULL
            GROUP BY TO_CHAR(po_warehouse_date, 'YYYY-MM'), commodity
            ORDER BY TO_CHAR(po_warehouse_date, 'YYYY-MM'), commodity
        `, params);

        res.json({
            items: itemsResult.rows,
            stats: statsResult.rows[0],
            vendorBreakdown: vendorResult.rows,
            commodityBreakdown: commodityResult.rows,
            monthlyBreakdown: monthlyResult.rows,
            monthlyByCommodity: monthlyByCommodityResult.rows,
            colorBreakdown: colorResult.rows
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Get dashboard summary stats
app.get('/api/dashboard', async function(req, res) {
    try {
        // Overall stats
        var overallResult = await pool.query(`
            SELECT
                COUNT(DISTINCT so_number) as total_orders,
                COUNT(DISTINCT customer) as total_customers,
                COUNT(DISTINCT style_number) as total_styles,
                SUM(quantity) as total_qty,
                SUM(total_amount) as total_dollars
            FROM order_items
            WHERE status = 'Open'
        `);

        // By customer
        var byCustomerResult = await pool.query(`
            SELECT customer, SUM(total_amount) as dollars, SUM(quantity) as qty, COUNT(DISTINCT so_number) as orders
            FROM order_items WHERE status = 'Open'
            GROUP BY customer ORDER BY SUM(total_amount) DESC LIMIT 10
        `);

        // By month
        var byMonthResult = await pool.query(`
            SELECT TO_CHAR(delivery_date, 'Mon YYYY') as month,
                   SUM(total_amount) as dollars,
                   SUM(quantity) as qty
            FROM order_items WHERE status = 'Open' AND delivery_date IS NOT NULL
            GROUP BY TO_CHAR(delivery_date, 'Mon YYYY'), DATE_TRUNC('month', delivery_date)
            ORDER BY DATE_TRUNC('month', delivery_date)
        `);

        // By commodity
        var byCommodityResult = await pool.query(`
            SELECT commodity, SUM(total_amount) as dollars, SUM(quantity) as qty
            FROM order_items WHERE status = 'Open' AND commodity IS NOT NULL
            GROUP BY commodity ORDER BY SUM(total_amount) DESC
        `);

        // Upcoming deliveries (next 14 days)
        var upcomingResult = await pool.query(`
            SELECT COUNT(DISTINCT so_number) as orders, SUM(total_amount) as dollars
            FROM order_items
            WHERE status = 'Open' AND delivery_date BETWEEN CURRENT_DATE AND CURRENT_DATE + INTERVAL '14 days'
        `);

        res.json({
            overall: overallResult.rows[0],
            byCustomer: byCustomerResult.rows,
            byMonth: byMonthResult.rows,
            byCommodity: byCommodityResult.rows,
            upcoming: upcomingResult.rows[0]
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ============================================
// ADMIN SETTINGS ENDPOINTS
// ============================================

// Get all settings
app.get('/api/settings', async function(req, res) {
    try {
        var result = await pool.query('SELECT setting_key, setting_value FROM app_settings');
        var settings = {};
        result.rows.forEach(function(row) {
            settings[row.setting_key] = row.setting_value;
        });
        res.json(settings);
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Update a setting
app.post('/api/settings', async function(req, res) {
    try {
        var key = req.body.key;
        var value = req.body.value;
        if (!key) return res.status(400).json({ error: 'Setting key required' });

        await pool.query(
            `INSERT INTO app_settings (setting_key, setting_value, updated_at)
             VALUES ($1, $2, NOW())
             ON CONFLICT (setting_key) DO UPDATE SET setting_value = $2, updated_at = NOW()`,
            [key, value]
        );
        res.json({ success: true, key: key, value: value });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Verify management PIN
app.post('/api/verify-pin', async function(req, res) {
    try {
        var pin = req.body.pin;
        if (!pin) return res.status(400).json({ error: 'PIN required' });
        var result = await pool.query("SELECT setting_value FROM app_settings WHERE setting_key = 'managementPin'");
        if (result.rows.length === 0 || !result.rows[0].setting_value) {
            // No PIN set = show to everyone
            return res.json({ valid: true });
        }
        res.json({ valid: pin === result.rows[0].setting_value });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Check if management PIN is configured
app.get('/api/pin-status', async function(req, res) {
    try {
        var result = await pool.query("SELECT setting_value FROM app_settings WHERE setting_key = 'managementPin'");
        var hasPin = result.rows.length > 0 && result.rows[0].setting_value && result.rows[0].setting_value.length > 0;
        res.json({ hasPin: hasPin });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ============================================
// ZOHO ANALYTICS SYNC ENDPOINTS
// ============================================

// OAuth callback for Zoho Analytics
app.get('/api/zoho/callback', async function(req, res) {
    try {
        var code = req.query.code;
        if (!code) {
            return res.status(400).send('Missing authorization code');
        }

        var clientId = process.env.ZOHO_CLIENT_ID;
        var clientSecret = process.env.ZOHO_CLIENT_SECRET;
        var redirectUri = (process.env.APP_URL || 'https://open-order-production.up.railway.app') + '/api/zoho/callback';

        var params = new URLSearchParams();
        params.append('code', code);
        params.append('client_id', clientId);
        params.append('client_secret', clientSecret);
        params.append('redirect_uri', redirectUri);
        params.append('grant_type', 'authorization_code');

        var response = await fetch('https://accounts.zoho.com/oauth/v2/token', {
            method: 'POST',
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
            body: params.toString()
        });

        var data = await response.json();
        console.log('OAuth response:', data);

        if (data.refresh_token) {
            // Store the refresh token
            await pool.query('INSERT INTO zoho_tokens (access_token, refresh_token, expires_at) VALUES ($1, $2, $3)',
                [data.access_token, data.refresh_token, new Date(Date.now() + data.expires_in * 1000)]);
            zohoAccessToken = data.access_token;
            res.send('<h1>Success!</h1><p>Zoho connected. Refresh token: <code>' + data.refresh_token + '</code></p><p>Add this to your Railway environment variables as ZOHO_REFRESH_TOKEN</p><p><a href="/">Back to Dashboard</a></p>');
        } else {
            res.status(400).send('Failed to get refresh token: ' + JSON.stringify(data));
        }
    } catch (err) {
        res.status(500).send('Error: ' + err.message);
    }
});

// Initiate Zoho OAuth flow
app.get('/api/zoho/connect', function(req, res) {
    var clientId = process.env.ZOHO_CLIENT_ID;
    var redirectUri = (process.env.APP_URL || 'https://open-order-production.up.railway.app') + '/api/zoho/callback';
    var scope = 'ZohoAnalytics.data.read,ZohoAnalytics.metadata.read,WorkDrive.files.ALL,WorkDrive.team.ALL,WorkDrive.teamfolders.ALL,ZohoBooks.salesorders.READ,ZohoBooks.purchaseorders.READ,ZohoBooks.invoices.READ';
    var authUrl = 'https://accounts.zoho.com/oauth/v2/auth?scope=' + scope + '&client_id=' + clientId + '&response_type=code&access_type=offline&redirect_uri=' + encodeURIComponent(redirectUri) + '&prompt=consent';
    res.redirect(authUrl);
});

// Zoho document deep link - looks up document by number and redirects to Zoho Inventory
app.get('/api/zoho-link/:type/:docNumber', async function(req, res) {
    try {
        var type = req.params.type;
        var docNumber = req.params.docNumber;
        var orgId = process.env.ZOHO_BOOKS_ORG_ID || '677681121';
        
        if (!zohoAccessToken) await refreshZohoToken();
        if (!zohoAccessToken) return res.json({ success: false, error: 'No Zoho token' });
        
        var apiPath, idField, numberField;
        if (type === 'purchaseorder') {
            apiPath = 'purchaseorders';
            idField = 'purchaseorder_id';
            numberField = 'purchaseorder_number';
        } else if (type === 'salesorder') {
            apiPath = 'salesorders';
            idField = 'salesorder_id';
            numberField = 'salesorder_number';
        } else if (type === 'invoice') {
            apiPath = 'invoices';
            idField = 'invoice_id';
            numberField = 'invoice_number';
        } else {
            return res.json({ success: false, error: 'Invalid type' });
        }
        
        var searchUrl = 'https://www.zohoapis.com/books/v3/' + apiPath + '?organization_id=' + orgId + '&search_text=' + encodeURIComponent(docNumber);
        var response = await fetch(searchUrl, {
            headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
        });
        
        if (response.status === 401) {
            await refreshZohoToken();
            response = await fetch(searchUrl, {
                headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
            });
        }
        
        var data = await response.json();
        var docs = data[apiPath] || [];
        var match = docs.find(function(d) { return d[numberField] === docNumber; });
        
        if (match) {
            var zohoUrl = 'https://inventory.zoho.com/app/' + orgId + '#/' + apiPath + '/' + match[idField];
            return res.json({ success: true, url: zohoUrl });
        }
        
        return res.json({ success: false, url: 'https://inventory.zoho.com/app/' + orgId + '#/' + apiPath, error: 'Document not found' });
    } catch (err) {
        console.error('Zoho link error:', err.message);
        res.json({ success: false, error: err.message, url: 'https://inventory.zoho.com/app/677681121#/' + (req.params.type === 'purchaseorder' ? 'purchaseorders' : req.params.type === 'salesorder' ? 'salesorders' : 'invoices') });
    }
});

// Test Zoho Analytics connection
app.post('/api/zoho-analytics/test', async function(req, res) {
    try {
        var result = await fetchZohoAnalyticsData();
        if (result.success) {
            var rowCount = result.data && result.data.data ? result.data.data.length : 0;
            var columns = result.data && result.data.column_order ? result.data.column_order : [];
            res.json({ success: true, message: 'Connected! Found ' + rowCount + ' rows with columns: ' + columns.slice(0, 10).join(', ') + (columns.length > 10 ? '...' : '') });
        } else {
            res.json({ success: false, error: result.error });
        }
    } catch (err) {
        res.json({ success: false, error: err.message });
    }
});

// Sync data from Zoho Analytics
app.post('/api/zoho-analytics/sync', async function(req, res) {
    try {
        var result = await syncFromZohoAnalytics();
        res.json(result);
    } catch (err) {
        res.json({ success: false, error: err.message });
    }
});

// Get Zoho Analytics status
app.get('/api/zoho-analytics/status', async function(req, res) {
    try {
        var hasCredentials = !!(process.env.ZOHO_CLIENT_ID && process.env.ZOHO_CLIENT_SECRET && process.env.ZOHO_REFRESH_TOKEN);
        var lastSync = await pool.query("SELECT created_at, records_imported FROM import_history WHERE import_type = 'zoho_analytics_sync' ORDER BY created_at DESC LIMIT 1");
        res.json({
            connected: hasCredentials,
            workspaceId: ZOHO_ANALYTICS_WORKSPACE_ID,
            viewId: ZOHO_ANALYTICS_VIEW_ID,
            lastSync: lastSync.rows.length > 0 ? lastSync.rows[0].created_at : null,
            lastSyncRecords: lastSync.rows.length > 0 ? lastSync.rows[0].records_imported : null
        });
    } catch (err) {
        res.json({ error: err.message });
    }
});

// ============================================
// WORKDRIVE FOLDER SYNC API ENDPOINTS
// ============================================

// Sync data from WorkDrive folder
app.post('/api/workdrive/sync', async function(req, res) {
    try {
        var force = req.query.force === 'true' || req.body.force === true;
        console.log('Manual WorkDrive folder sync triggered (force=' + force + ')');
        var result = await syncFromWorkDriveFolder(force);
        res.json(result);
    } catch (err) {
        console.error('WorkDrive sync error:', err);
        res.json({ success: false, error: err.message });
    }
});

// Get WorkDrive sync status
app.get('/api/workdrive/status', async function(req, res) {
    try {
        var isConfigured = !!(WORKDRIVE_SYNC_FOLDER_ID);

        // Get last sync info
        var lastSync = await pool.query(
            "SELECT created_at, records_imported, error_message FROM import_history WHERE import_type = 'workdrive_sync' ORDER BY created_at DESC LIMIT 1"
        );

        // Check what files are in the folder
        var folderFiles = [];
        var latestFileName = null;
        if (isConfigured) {
            try {
                var listResult = await listWorkDriveFolder(WORKDRIVE_SYNC_FOLDER_ID);
                if (listResult.success) {
                    folderFiles = listResult.files
                        .filter(function(f) { return f.attributes && f.attributes.name && f.attributes.name.toLowerCase().endsWith('.csv'); })
                        .map(function(f) { return { name: f.attributes.name, modified: f.attributes.modified_time }; })
                        .slice(0, 5);
                    var latestFile = findLatestCSV(listResult.files);
                    if (latestFile) {
                        latestFileName = latestFile.attributes.name;
                    }
                }
            } catch (err) {
                console.log('Could not list WorkDrive folder:', err.message);
            }
        }

        res.json({
            configured: isConfigured,
            folderId: WORKDRIVE_SYNC_FOLDER_ID || null,
            lastSync: lastSync.rows.length > 0 ? lastSync.rows[0].created_at : null,
            lastSyncRecords: lastSync.rows.length > 0 ? lastSync.rows[0].records_imported : null,
            lastSyncFile: lastSync.rows.length > 0 ? lastSync.rows[0].error_message : null,
            latestAvailableFile: latestFileName,
            recentFiles: folderFiles
        });
    } catch (err) {
        res.json({ error: err.message });
    }
});

// ============================================
// IMPORT PO SYNC ENDPOINTS
// ============================================

// Sync Import POs from WorkDrive folder
app.post('/api/workdrive/import-po/sync', async function(req, res) {
    try {
        var force = req.query.force === 'true' || req.body.force === true;
        console.log('Manual Import PO sync triggered (force=' + force + ')');
        var result = await syncImportPOsFromWorkDrive(force);
        res.json(result);
    } catch (err) {
        console.error('Import PO sync error:', err);
        res.json({ success: false, error: err.message });
    }
});

// Get Import PO sync status
app.get('/api/workdrive/import-po/status', async function(req, res) {
    try {
        var isConfigured = !!(WORKDRIVE_IMPORT_PO_FOLDER_ID);

        // Get last sync info
        var lastSync = await pool.query(
            "SELECT created_at, records_imported, error_message FROM import_history WHERE import_type = 'import_po_sync' ORDER BY created_at DESC LIMIT 1"
        );

        // Check what files are in the folder
        var folderFiles = [];
        var latestFileName = null;
        if (isConfigured) {
            try {
                var listResult = await listWorkDriveFolder(WORKDRIVE_IMPORT_PO_FOLDER_ID);
                if (listResult.success) {
                    folderFiles = listResult.files
                        .filter(function(f) { return f.attributes && f.attributes.name && f.attributes.name.toLowerCase().endsWith('.csv'); })
                        .map(function(f) { return { name: f.attributes.name, modified: f.attributes.modified_time }; })
                        .slice(0, 5);
                    var latestFile = findLatestCSV(listResult.files);
                    if (latestFile) {
                        latestFileName = latestFile.attributes.name;
                    }
                }
            } catch (err) {
                console.log('Could not list Import PO folder:', err.message);
            }
        }

        res.json({
            configured: isConfigured,
            folderId: WORKDRIVE_IMPORT_PO_FOLDER_ID || null,
            lastSync: lastSync.rows.length > 0 ? lastSync.rows[0].created_at : null,
            lastSyncRecords: lastSync.rows.length > 0 ? lastSync.rows[0].records_imported : null,
            lastSyncFile: lastSync.rows.length > 0 ? lastSync.rows[0].error_message : null,
            latestAvailableFile: latestFileName,
            recentFiles: folderFiles
        });
    } catch (err) {
        res.json({ error: err.message });
    }
});

// List files in WorkDrive folder
app.get('/api/workdrive/files', async function(req, res) {
    try {
        if (!WORKDRIVE_SYNC_FOLDER_ID) {
            return res.json({ success: false, error: 'WorkDrive folder not configured' });
        }

        var result = await listWorkDriveFolder(WORKDRIVE_SYNC_FOLDER_ID);
        if (!result.success) {
            return res.json(result);
        }

        // Filter to just CSV files and return relevant info
        var csvFiles = result.files
            .filter(function(f) { return f.attributes && f.attributes.name && f.attributes.name.toLowerCase().endsWith('.csv'); })
            .map(function(f) {
                return {
                    id: f.id,
                    name: f.attributes.name,
                    modified: f.attributes.modified_time,
                    size: f.attributes.storage_info ? f.attributes.storage_info.size : null
                };
            });

        res.json({ success: true, files: csvFiles });
    } catch (err) {
        res.json({ success: false, error: err.message });
    }
});

// Browse WorkDrive folders (to help find folder ID)
app.get('/api/workdrive/browse', async function(req, res) {
    try {
        if (!zohoAccessToken) {
            var tokenResult = await refreshZohoToken(true);
            if (!tokenResult.success) {
                return res.json({ success: false, error: 'Failed to get Zoho token' });
            }
        }

        var folderId = req.query.folderId;

        // If no folder specified, get user's root folders
        if (!folderId) {
            // First get current user info to find their team
            var userUrl = 'https://workdrive.zoho.com/api/v1/users/me';
            var userRes = await fetch(userUrl, {
                headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
            });

            if (userRes.status === 401) {
                await refreshZohoToken(true);
                userRes = await fetch(userUrl, {
                    headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
                });
            }

            console.log('User API status:', userRes.status);
            var userData = null;
            var teamId = null;

            if (userRes.ok) {
                userData = await userRes.json();
                console.log('User data:', JSON.stringify(userData).substring(0, 500));
                // Try to extract team ID from user data
                if (userData.data && userData.data.attributes) {
                    teamId = userData.data.attributes.team_id;
                }
            }

            // If we have a team ID, get team folders
            if (teamId) {
                var teamFoldersUrl = 'https://workdrive.zoho.com/api/v1/teams/' + teamId + '/teamfolders';
                var foldersRes = await fetch(teamFoldersUrl, {
                    headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
                });

                if (foldersRes.ok) {
                    var foldersData = await foldersRes.json();
                    var folders = (foldersData.data || []).map(function(f) {
                        return {
                            id: f.id,
                            name: f.attributes ? f.attributes.name : 'Unknown',
                            type: 'team_folder'
                        };
                    });

                    return res.json({ success: true, teamId: teamId, folders: folders, path: '/' });
                }
            }

            // Fallback: try to list all accessible files/folders using search
            // Or get the user's private space
            var privateUrl = 'https://workdrive.zoho.com/api/v1/privatespace';
            var privateRes = await fetch(privateUrl, {
                headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
            });

            if (privateRes.ok) {
                var privateData = await privateRes.json();
                console.log('Private space data:', JSON.stringify(privateData).substring(0, 500));
                if (privateData.data && privateData.data.id) {
                    // Got private space, list its contents
                    var psUrl = 'https://workdrive.zoho.com/api/v1/files/' + privateData.data.id + '/files';
                    var psRes = await fetch(psUrl, {
                        headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
                    });

                    if (psRes.ok) {
                        var psData = await psRes.json();
                        var items = (psData.data || []).map(function(f) {
                            var isFolder = f.attributes && (f.attributes.type === 'folder' || !f.attributes.extension);
                            return {
                                id: f.id,
                                name: f.attributes ? f.attributes.name : 'Unknown',
                                type: isFolder ? 'folder' : 'file'
                            };
                        });
                        return res.json({ success: true, folders: items, path: '/Private Space', privateSpaceId: privateData.data.id });
                    }
                }
            }

            // Last resort: return instructions for manual folder ID entry
            return res.json({
                success: false,
                error: 'Could not browse folders automatically. Please find the folder in WorkDrive, copy the folder ID from the URL (after /folder/), and enter it manually.',
                hint: 'The folder ID looks like: ly8za0b5f93e4b9e5462b9e0c4a9f2e8b9c0d'
            });
        }

        // Get team ID from query param or env
        var teamId = req.query.teamId || WORKDRIVE_TEAM_ID;

        // Browse a specific folder - try multiple API formats
        var urls = [
            'https://workdrive.zoho.com/api/v1/files/' + folderId + '/files',
            'https://workdrive.zoho.com/api/v1/teamfolders/' + folderId + '/files'
        ];

        var response = null;
        var lastError = null;
        var responseText = null;

        for (var urlIdx = 0; urlIdx < urls.length; urlIdx++) {
            var url = urls[urlIdx];
            console.log('Trying WorkDrive URL:', url, 'teamId:', teamId);

            var headers = { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken };
            if (teamId) {
                headers['x-team-id'] = teamId;
            }

            response = await fetch(url, { headers: headers });

            console.log('Response status:', response.status);

            if (response.status === 401) {
                await refreshZohoToken(true);
                headers['Authorization'] = 'Zoho-oauthtoken ' + zohoAccessToken;
                response = await fetch(url, { headers: headers });
            }

            if (response.ok) {
                console.log('Success with URL:', url);
                break;
            } else {
                lastError = response.status;
                try {
                    responseText = await response.text();
                    console.log('Error response:', responseText.substring(0, 200));
                } catch(e) {}
            }
        }

        if (!response || !response.ok) {
            return res.json({ success: false, error: 'API error: ' + lastError + '. The folder may require different permissions.' });
        }

        var data = await response.json();
        var items = (data.data || []).map(function(f) {
            var isFolder = f.attributes && (f.attributes.type === 'folder' || !f.attributes.extension);
            return {
                id: f.id,
                name: f.attributes ? f.attributes.name : 'Unknown',
                type: isFolder ? 'folder' : 'file',
                modified: f.attributes ? f.attributes.modified_time : null
            };
        });

        // Sort folders first, then files
        items.sort(function(a, b) {
            if (a.type === 'folder' && b.type !== 'folder') return -1;
            if (a.type !== 'folder' && b.type === 'folder') return 1;
            return (a.name || '').localeCompare(b.name || '');
        });

        res.json({ success: true, folderId: folderId, items: items });
    } catch (err) {
        console.error('Browse error:', err);
        res.json({ success: false, error: err.message });
    }
});

// CSV Import endpoint
app.post('/api/import', upload.single('file'), async function(req, res) {
    try {
        if (!req.file) {
            return res.status(400).json({ error: 'No file uploaded' });
        }

        var content = req.file.buffer.toString('utf-8');
        var lines = content.split('\n').filter(line => line.trim());

        if (lines.length < 2) {
            return res.status(400).json({ error: 'CSV file is empty or has no data rows' });
        }

        // Parse header row
        var headerRow = lines[0];
        var headers = parseCSVLine(headerRow).map(h => h.toLowerCase().trim().replace(/[^a-z0-9_]/g, '_'));

        console.log('CSV Headers:', headers);

        // Map expected columns (flexible matching)
        var colMap = {
            so_number: findColumn(headers, ['sales_order_number', 'so_number', 'so', 'sales_order', 'order_number', 'order_no', 'salesorder']),
            customer: findColumn(headers, ['customer_name', 'customer', 'cust', 'buyer', 'account']),
            style_number: findColumn(headers, ['style_name', 'style_number', 'style', 'style_no', 'sku', 'item', 'item_number']),
            style_name: findColumn(headers, ['style_suffix', 'style_name', 'name', 'description', 'item_name', 'product_name']),
            commodity: findColumn(headers, ['copmmodity', 'commodity', 'category', 'product_type', 'type', 'fabric', 'fabric_type']),
            color: findColumn(headers, ['color', 'colour', 'color_name', 'colorway']),
            quantity: findColumn(headers, ['quantity', 'qty', 'units', 'order_qty', 'ordered_qty']),
            unit_price: findColumn(headers, ['so_price_fcy', 'so_price_bcy', 'unit_price', 'price', 'unit_cost', 'rate']),
            total_amount: findColumn(headers, ['total_fcy', 'total_bcy', 'total_amount', 'total', 'amount', 'line_total', 'ext_price', 'extended_price', 'total_price']),
            delivery_date: findColumn(headers, ['so_cancel_date', 'delivery_date', 'delivery', 'ship_date', 'due_date', 'eta', 'expected_date']),
            status: findColumn(headers, ['so_status', 'status', 'order_status', 'state']),
            image_url: findColumn(headers, ['style_image', 'image_url', 'image', 'image_link', 'workdrive_link', 'picture', 'photo_url']),
            // PO columns
            po_number: findColumn(headers, ['purchase_order_number', 'po_number', 'po', 'import_po']),
            vendor_name: findColumn(headers, ['vendor_name', 'vendor', 'supplier', 'factory']),
            po_status: findColumn(headers, ['po_status', 'purchase_status']),
            po_quantity: findColumn(headers, ['po_quantity', 'po_qty']),
            po_unit_price: findColumn(headers, ['po_price_fcy', 'po_unit_price', 'po_price']),
            po_total: findColumn(headers, ['po_total_fcy', 'po_total', 'po_amount']),
            po_warehouse_date: findColumn(headers, ['in_warehouse_date', 'po_warehouse_date', 'warehouse_date', 'eta_warehouse'])
        };

        console.log('Column mapping:', colMap);

        // Validate required columns
        if (colMap.so_number === -1) return res.status(400).json({ error: 'Missing required column: SO Number' });
        if (colMap.customer === -1) return res.status(400).json({ error: 'Missing required column: Customer' });
        if (colMap.style_number === -1) return res.status(400).json({ error: 'Missing required column: Style Number' });

        // Clear existing data (full refresh) - ALWAYS clear on new import
        var clearMode = req.body.clearMode || req.query.clearMode || 'replace';
        console.log('Import clearMode:', clearMode);

        // Count existing rows before clearing
        var countBefore = await pool.query('SELECT COUNT(*) as cnt FROM order_items');
        console.log('Rows BEFORE clear:', countBefore.rows[0].cnt);

        if (clearMode === 'replace') {
            // Use TRUNCATE for faster, more reliable clearing
            await pool.query('TRUNCATE TABLE order_items RESTART IDENTITY CASCADE');
            await pool.query('TRUNCATE TABLE sales_orders RESTART IDENTITY CASCADE');
            console.log('✓ TRUNCATED order_items and sales_orders tables');
        }

        // Verify clear worked
        var countAfter = await pool.query('SELECT COUNT(*) as cnt FROM order_items');
        console.log('Rows AFTER clear:', countAfter.rows[0].cnt);

        var imported = 0;
        var errors = [];

        for (var i = 1; i < lines.length; i++) {
            try {
                var values = parseCSVLine(lines[i]);
                if (values.length < 3) continue; // Skip empty/malformed rows

                var soNumber = getValue(values, colMap.so_number) || '';
                var customer = getValue(values, colMap.customer) || '';
                var styleNumber = getValue(values, colMap.style_number) || '';

                if (!soNumber || !customer || !styleNumber) continue;

                var styleName = getValue(values, colMap.style_name) || styleNumber;
                var commodity = getValue(values, colMap.commodity) || '';
                var color = getValue(values, colMap.color) || '';
                var quantity = parseNumber(getValue(values, colMap.quantity)) || 0;
                var unitPrice = parseNumber(getValue(values, colMap.unit_price)) || 0;
                var totalAmount = parseNumber(getValue(values, colMap.total_amount)) || (quantity * unitPrice);
                var deliveryDate = parseDate(getValue(values, colMap.delivery_date));
                var status = getValue(values, colMap.status) || 'Open';
                var imageUrl = getValue(values, colMap.image_url) || '';

                // PO data
                var poNumber = getValue(values, colMap.po_number) || '';
                var vendorName = getValue(values, colMap.vendor_name) || '';
                var poStatus = getValue(values, colMap.po_status) || '';
                var poQuantity = parseNumber(getValue(values, colMap.po_quantity)) || 0;
                var poUnitPrice = parseNumber(getValue(values, colMap.po_unit_price)) || 0;
                var poTotal = parseNumber(getValue(values, colMap.po_total)) || 0;
                var poWarehouseDate = parseDate(getValue(values, colMap.po_warehouse_date));

                // Normalize SO status - Open, Partial, Invoiced
                var statusLower = status.toLowerCase();
                if (statusLower.includes('partial')) {
                    status = 'Partial';
                } else if (statusLower.includes('invoice') || statusLower.includes('shipped') || statusLower.includes('complete') || statusLower.includes('billed')) {
                    status = 'Invoiced';
                } else {
                    status = 'Open';
                }

                // Normalize PO status - Open, Received (Billed)
                if (poStatus) {
                    var poStatusLower = poStatus.toLowerCase();
                    if (poStatusLower.includes('billed') || poStatusLower.includes('received') || poStatusLower.includes('complete')) {
                        poStatus = 'Received';
                    } else {
                        poStatus = 'Open';
                    }
                }

                await pool.query(`
                    INSERT INTO order_items (so_number, customer, style_number, style_name, commodity, color, quantity, unit_price, total_amount, delivery_date, status, image_url, po_number, vendor_name, po_status, po_quantity, po_unit_price, po_total, po_warehouse_date, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, NOW())
                `, [soNumber, customer, styleNumber, styleName, commodity, color, quantity, unitPrice, totalAmount, deliveryDate, status, imageUrl, poNumber, vendorName, poStatus, poQuantity, poUnitPrice, poTotal, poWarehouseDate]);

                imported++;
            } catch (rowErr) {
                errors.push({ row: i + 1, error: rowErr.message });
            }
        }

        // Update sales_orders summary table
        await pool.query(`
            INSERT INTO sales_orders (so_number, customer, delivery_date, status, total_amount, total_qty, updated_at)
            SELECT so_number, customer, MIN(delivery_date),
                   CASE WHEN COUNT(*) FILTER (WHERE status = 'Open') > 0 THEN 'Open' ELSE 'Invoiced' END,
                   SUM(total_amount), SUM(quantity), NOW()
            FROM order_items
            GROUP BY so_number, customer
            ON CONFLICT (so_number) DO UPDATE SET
                customer = EXCLUDED.customer,
                delivery_date = EXCLUDED.delivery_date,
                status = EXCLUDED.status,
                total_amount = EXCLUDED.total_amount,
                total_qty = EXCLUDED.total_qty,
                updated_at = NOW()
        `);

        // Log import
        await pool.query('INSERT INTO import_history (import_type, status, records_imported) VALUES ($1, $2, $3)',
            ['csv', 'success', imported]);

        // Get validation stats for the response
        var validationStats = await pool.query(`
            SELECT
                COUNT(*) as total_rows,
                COUNT(*) FILTER (WHERE status = 'open' OR status = 'Open' OR status = 'partially_invoiced') as open_rows,
                SUM(total_amount) as total_value,
                SUM(total_amount) FILTER (WHERE status = 'open' OR status = 'Open' OR status = 'partially_invoiced') as open_value
            FROM order_items
        `);
        var stats = validationStats.rows[0];

        console.log('=== IMPORT VALIDATION ===');
        console.log('Total rows imported:', stats.total_rows);
        console.log('Open rows:', stats.open_rows);
        console.log('Total value: $' + (parseFloat(stats.total_value) || 0).toLocaleString());
        console.log('Open value: $' + (parseFloat(stats.open_value) || 0).toLocaleString());

        res.json({
            success: true,
            imported: imported,
            errors: errors.slice(0, 10),
            message: `Imported ${imported} line items`,
            validation: {
                totalRows: parseInt(stats.total_rows),
                openRows: parseInt(stats.open_rows),
                totalValue: parseFloat(stats.total_value) || 0,
                openValue: parseFloat(stats.open_value) || 0
            }
        });

    } catch (err) {
        console.error('Import error:', err);
        await pool.query('INSERT INTO import_history (import_type, status, error_message) VALUES ($1, $2, $3)',
            ['csv', 'failed', err.message]);
        res.status(500).json({ error: err.message });
    }
});

// Helper functions for CSV parsing
function parseCSVLine(line) {
    var result = [];
    var current = '';
    var inQuotes = false;
    for (var i = 0; i < line.length; i++) {
        var char = line[i];
        if (char === '"') {
            if (inQuotes && line[i + 1] === '"') {
                current += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
        } else if (char === ',' && !inQuotes) {
            result.push(current.trim());
            current = '';
        } else {
            current += char;
        }
    }
    result.push(current.trim());
    return result;
}

function findColumn(headers, aliases) {
    var headersLower = headers.map(function(h) { return h.toLowerCase().replace(/[^a-z0-9]/g, '_'); });
    for (var alias of aliases) {
        var aliasLower = alias.toLowerCase().replace(/[^a-z0-9]/g, '_');
        var idx = headersLower.indexOf(aliasLower);
        if (idx !== -1) return idx;
    }
    return -1;
}

function getValue(values, index) {
    if (index === -1 || index >= values.length) return '';
    return (values[index] || '').replace(/^["']|["']$/g, '').trim();
}

function parseNumber(val) {
    if (!val) return 0;
    var num = parseFloat(val.replace(/[^0-9.-]/g, ''));
    return isNaN(num) ? 0 : num;
}

function parseDate(val) {
    if (!val) return null;
    try {
        var date = new Date(val);
        if (isNaN(date.getTime())) return null;
        return date.toISOString().split('T')[0];
    } catch (e) {
        return null;
    }
}

// ============================================
// AI CHAT ENDPOINT
// ============================================
app.post('/api/chat', async function(req, res) {
    try {
        var message = req.body.message;
        if (!message) return res.status(400).json({ error: 'Message required' });

        if (!process.env.ANTHROPIC_API_KEY) {
            return res.json({ response: "AI chat requires ANTHROPIC_API_KEY to be configured.", filters: null });
        }

        // Get current data context
        var statsResult = await pool.query(`
            SELECT COUNT(DISTINCT customer) as customers, COUNT(DISTINCT commodity) as commodities,
                   COUNT(DISTINCT TO_CHAR(delivery_date, 'YYYY-MM')) as months
            FROM order_items WHERE status = 'Open'
        `);
        var customersResult = await pool.query('SELECT DISTINCT customer FROM order_items WHERE status = \'Open\' ORDER BY customer');
        var commoditiesResult = await pool.query('SELECT DISTINCT commodity FROM order_items WHERE status = \'Open\' AND commodity IS NOT NULL ORDER BY commodity');
        var monthsResult = await pool.query("SELECT DISTINCT TO_CHAR(delivery_date, 'YYYY-MM') as month FROM order_items WHERE status = 'Open' AND delivery_date IS NOT NULL ORDER BY month");

        var customers = customersResult.rows.map(r => r.customer);
        var commodities = commoditiesResult.rows.map(r => r.commodity);
        var months = monthsResult.rows.map(r => r.month);

        var systemPrompt = `You are a helpful assistant for a sales order dashboard. You CAN and WILL filter the data and show results on screen.

IMPORTANT: You have the ABILITY to filter and display orders. When users ask questions, APPLY the filters and show results.

Available data:
- Customers: ${customers.slice(0, 20).join(', ')}${customers.length > 20 ? '...' : ''}
- Commodities: ${commodities.join(', ')}
- Delivery Months: ${months.join(', ')}

ALWAYS respond with a brief message AND a JSON block to apply filters. The filters will automatically update the dashboard.

Example - User asks "Show Ross orders":
"Filtering to Ross Stores orders now.
\`\`\`json
{"customer": "Ross Stores", "action": "filter"}
\`\`\`"

Example - User asks "Which month has the most dresses?":
"Filtering to show all Dress orders so you can see the monthly breakdown.
\`\`\`json
{"commodity": "Dress", "action": "filter"}
\`\`\`"

Example - User asks "Show Burlington's denim for June":
"Here are Burlington's Denim orders for June 2025.
\`\`\`json
{"customer": "Burlington", "commodity": "Denim", "month": "2025-06", "action": "filter"}
\`\`\`"

RULES:
1. ALWAYS include the JSON block - this triggers the actual filter
2. Keep your text response SHORT (1-2 sentences max)
3. Match names flexibly (case-insensitive, partial matches)
4. Convert month names to YYYY-MM format
5. If asked about analytics/comparisons, filter to relevant data so user can see it on the dashboard`;

        var response = await fetch('https://api.anthropic.com/v1/messages', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'x-api-key': process.env.ANTHROPIC_API_KEY,
                'anthropic-version': '2023-06-01'
            },
            body: JSON.stringify({
                model: 'claude-sonnet-4-20250514',
                max_tokens: 1024,
                system: systemPrompt,
                messages: [{ role: 'user', content: message }]
            })
        });

        var data = await response.json();
        var aiResponse = data.content && data.content[0] ? data.content[0].text : 'Sorry, I could not process that request.';

        // Extract JSON filters from response
        var filters = null;
        var jsonMatch = aiResponse.match(/```json\s*([\s\S]*?)\s*```/);
        if (jsonMatch) {
            try {
                filters = JSON.parse(jsonMatch[1]);
                // Match customer name flexibly
                if (filters.customer) {
                    var matchedCustomer = customers.find(c => c.toLowerCase().includes(filters.customer.toLowerCase()));
                    if (matchedCustomer) filters.customer = matchedCustomer;
                }
                // Match commodity flexibly
                if (filters.commodity) {
                    var matchedCommodity = commodities.find(c => c.toLowerCase().includes(filters.commodity.toLowerCase()));
                    if (matchedCommodity) filters.commodity = matchedCommodity;
                }
            } catch (e) {
                console.log('Could not parse filters JSON');
            }
        }

        res.json({ response: aiResponse.replace(/```json[\s\S]*?```/g, '').trim(), filters: filters });

    } catch (err) {
        console.error('Chat error:', err);
        res.status(500).json({ error: err.message });
    }
});

// ============================================
// IMAGE PROXY (for WorkDrive images)
// ============================================

// In-memory LRU cache for images (much faster than DB queries)
var memoryCache = new Map();
var MEMORY_CACHE_MAX = 200; // Keep last 200 images in memory
var memoryCacheHits = 0;
var memoryCacheMisses = 0;

function addToMemoryCache(fileId, data, contentType) {
    // Simple LRU: if at max, delete oldest entry
    if (memoryCache.size >= MEMORY_CACHE_MAX) {
        var oldestKey = memoryCache.keys().next().value;
        memoryCache.delete(oldestKey);
    }
    memoryCache.set(fileId, { data: data, contentType: contentType, etag: '"' + fileId + '"' });
}

app.get('/api/image/:fileId', async function(req, res) {
    try {
        var fileId = req.params.fileId;
        var etag = '"' + fileId + '"';

        // Check If-None-Match header (browser asking if cache is still valid)
        if (req.headers['if-none-match'] === etag) {
            return res.status(304).end(); // Not Modified - browser uses cached version
        }

        // 1. Check in-memory cache first (fastest)
        if (memoryCache.has(fileId)) {
            memoryCacheHits++;
            var cached = memoryCache.get(fileId);
            // Move to end (most recently used)
            memoryCache.delete(fileId);
            memoryCache.set(fileId, cached);

            res.set('Content-Type', cached.contentType || 'image/jpeg');
            res.set('Cache-Control', 'public, max-age=31536000, immutable');
            res.set('ETag', etag);
            return res.send(cached.data);
        }

        memoryCacheMisses++;

        // 2. Check database cache (slower but persistent)
        var cacheResult = await pool.query(
            'SELECT image_data, content_type FROM image_cache WHERE file_id = $1',
            [fileId]
        );

        if (cacheResult.rows.length > 0 && cacheResult.rows[0].image_data) {
            var imgData = cacheResult.rows[0].image_data;
            var contentType = cacheResult.rows[0].content_type || 'image/jpeg';

            // Add to memory cache for next time
            addToMemoryCache(fileId, imgData, contentType);

            res.set('Content-Type', contentType);
            res.set('Cache-Control', 'public, max-age=31536000, immutable');
            res.set('ETag', etag);
            return res.send(imgData);
        }

        // 3. Cache miss - need to fetch from Zoho
        console.log('○ Full MISS, fetching from Zoho:', fileId);
        var result = await fetchZohoImage(fileId);

        // Store in database
        try {
            await pool.query(
                'INSERT INTO image_cache (file_id, image_data, content_type) VALUES ($1, $2, $3) ON CONFLICT (file_id) DO UPDATE SET image_data = $2, content_type = $3',
                [fileId, result.buffer, result.contentType]
            );
        } catch (cacheErr) {
            console.log('✗ DB Cache FAILED:', fileId, cacheErr.message);
        }

        // Add to memory cache
        addToMemoryCache(fileId, result.buffer, result.contentType);

        res.set('Content-Type', result.contentType);
        res.set('Cache-Control', 'public, max-age=31536000, immutable');
        res.set('ETag', etag);
        res.send(result.buffer);

    } catch (err) {
        console.error('Image fetch error for', req.params.fileId + ':', err.message);
        res.status(500).json({ error: err.message, fileId: req.params.fileId });
    }
});

// Endpoint to check cache stats
app.get('/api/cache/stats', function(req, res) {
    res.json({
        memoryCache: {
            size: memoryCache.size,
            maxSize: MEMORY_CACHE_MAX,
            hits: memoryCacheHits,
            misses: memoryCacheMisses,
            hitRate: memoryCacheHits + memoryCacheMisses > 0 ?
                (memoryCacheHits / (memoryCacheHits + memoryCacheMisses) * 100).toFixed(1) + '%' : '0%'
        }
    });
});

// Background image pre-caching (runs in background, doesn't block)
var preCacheRunning = false;
var preCacheProgress = { total: 0, done: 0, errors: 0 };

async function preCacheImages() {
    if (preCacheRunning) {
        console.log('Pre-cache already running, skipping...');
        return;
    }

    preCacheRunning = true;
    preCacheProgress = { total: 0, done: 0, errors: 0, phase: 'refreshing token' };
    console.log('Starting background image pre-cache...');

    try {
        // Step 1: Force refresh the token before starting
        console.log('Force refreshing Zoho token before pre-cache...');
        var tokenResult = await refreshZohoToken(true); // Force refresh
        if (!tokenResult.success) {
            preCacheProgress.phase = 'token error: ' + (tokenResult.error || 'refresh failed');
            console.log('Pre-cache aborted: could not refresh token - ' + tokenResult.error);
            preCacheRunning = false;
            return;
        }
        console.log('Token refreshed successfully, starting pre-cache...');

        // Step 2: Find uncached images
        preCacheProgress.phase = 'discovering';
        console.log('Finding uncached images...');
        var result = await pool.query(`
            WITH image_file_ids AS (
                SELECT DISTINCT
                    SUBSTRING(image_url FROM '/download/([a-zA-Z0-9]+)') as file_id
                FROM order_items
                WHERE image_url IS NOT NULL
                AND image_url != ''
                AND image_url LIKE '%/download/%'
            )
            SELECT file_id
            FROM image_file_ids i
            WHERE file_id IS NOT NULL
            AND NOT EXISTS (
                SELECT 1 FROM image_cache c WHERE c.file_id = i.file_id
            )
        `);

        var uncached = result.rows.map(r => r.file_id);
        preCacheProgress = { total: uncached.length, done: 0, errors: 0, consecutive401s: 0, phase: 'caching' };
        console.log('Found ' + uncached.length + ' images to pre-cache');

        // Step 3: Process images ONE AT A TIME to avoid parallel 401 issues
        for (var i = 0; i < uncached.length; i++) {
            var fileId = uncached[i];

            try {
                var imgResult = await fetchZohoImage(fileId);
                await pool.query(
                    'INSERT INTO image_cache (file_id, image_data, content_type) VALUES ($1, $2, $3) ON CONFLICT (file_id) DO UPDATE SET image_data = $2, content_type = $3',
                    [fileId, imgResult.buffer, imgResult.contentType]
                );
                preCacheProgress.done++;
                preCacheProgress.consecutive401s = 0; // Reset on success

                // Log progress every 50 images
                if (preCacheProgress.done % 50 === 0) {
                    console.log('Pre-cache progress: ' + preCacheProgress.done + '/' + preCacheProgress.total + ' (' + preCacheProgress.errors + ' errors)');
                }
            } catch (err) {
                preCacheProgress.errors++;
                console.log('Pre-cache error: ' + fileId + ' - ' + err.message);

                // Track consecutive 401 errors
                if (err.message.includes('401')) {
                    preCacheProgress.consecutive401s++;

                    // If we get 5 consecutive 401s, try to refresh token once
                    if (preCacheProgress.consecutive401s === 5) {
                        console.log('5 consecutive 401 errors, attempting token refresh...');
                        var refreshResult = await refreshZohoToken(true);
                        if (!refreshResult.success) {
                            preCacheProgress.phase = 'stopped: token invalid';
                            console.log('Pre-cache stopped: token refresh failed after repeated 401s');
                            break;
                        }
                        console.log('Token refreshed, continuing pre-cache...');
                        preCacheProgress.consecutive401s = 0;
                    }

                    // If we still get 401s after refresh, stop
                    if (preCacheProgress.consecutive401s >= 10) {
                        preCacheProgress.phase = 'stopped: persistent 401 errors';
                        console.log('Pre-cache stopped: too many consecutive 401 errors');
                        break;
                    }
                } else {
                    preCacheProgress.consecutive401s = 0; // Reset for non-401 errors
                }
            }

            // Small delay between each image to avoid rate limits
            if (i < uncached.length - 1) {
                await new Promise(r => setTimeout(r, 200));
            }
        }

        if (!preCacheProgress.phase.startsWith('stopped')) {
            preCacheProgress.phase = 'complete';
        }
        console.log('Pre-cache finished! Done: ' + preCacheProgress.done + ', Errors: ' + preCacheProgress.errors);
    } catch (err) {
        console.error('Pre-cache failed:', err.message);
        preCacheProgress.phase = 'error: ' + err.message;
    } finally {
        preCacheRunning = false;
    }
}

// Endpoint to trigger pre-caching
app.post('/api/images/precache', async function(req, res) {
    if (preCacheRunning) {
        return res.json({ status: 'already_running', progress: preCacheProgress });
    }

    // Start in background
    preCacheImages();
    res.json({ status: 'started', message: 'Pre-caching started in background' });
});

// Endpoint to check pre-cache progress
app.get('/api/images/precache/status', function(req, res) {
    res.json({
        running: preCacheRunning,
        progress: preCacheProgress,
        phase: preCacheProgress.phase || 'idle'
    });
});

// Zoho status and cache stats
app.get('/api/zoho/status', async function(req, res) {
    try {
        var hasCredentials = !!(process.env.ZOHO_CLIENT_ID && process.env.ZOHO_CLIENT_SECRET && process.env.ZOHO_REFRESH_TOKEN);

        // Get cached image count and size
        var cacheStats = await pool.query(`
            SELECT
                COUNT(*) as count,
                COALESCE(SUM(LENGTH(image_data)), 0) as total_bytes
            FROM image_cache
            WHERE image_data IS NOT NULL
        `);

        // Get count of unique styles with image URLs
        var stylesWithImages = await pool.query(`
            SELECT COUNT(DISTINCT style_number) as count
            FROM order_items
            WHERE image_url IS NOT NULL AND image_url != ''
        `);

        // Get count of styles that have a Zoho file ID that's already cached
        var stylesCached = await pool.query(`
            SELECT COUNT(DISTINCT o.style_number) as count
            FROM order_items o
            JOIN image_cache c ON o.image_url LIKE '%' || c.file_id || '%'
            WHERE o.image_url IS NOT NULL AND o.image_url != ''
        `);

        var cachedCount = parseInt(cacheStats.rows[0].count);
        var cacheSizeBytes = parseInt(cacheStats.rows[0].total_bytes);
        var cacheSizeMB = (cacheSizeBytes / (1024 * 1024)).toFixed(2);

        res.json({
            configured: hasCredentials,
            connected: !!zohoAccessToken,
            lastTokenError: lastTokenRefreshError,
            lastRefreshAttempt: lastTokenRefreshAttempt ? new Date(lastTokenRefreshAttempt).toISOString() : null,
            cache: {
                cachedImages: cachedCount,
                cacheSizeMB: parseFloat(cacheSizeMB),
                stylesWithImageUrls: parseInt(stylesWithImages.rows[0].count),
                stylesCached: parseInt(stylesCached.rows[0].count)
            }
        });
    } catch (err) {
        console.error('Zoho status error:', err.message);
        res.json({
            configured: !!(process.env.ZOHO_CLIENT_ID && process.env.ZOHO_CLIENT_SECRET && process.env.ZOHO_REFRESH_TOKEN),
            connected: !!zohoAccessToken,
            lastTokenError: lastTokenRefreshError || err.message,
            cache: { cachedImages: 0, cacheSizeMB: 0, stylesWithImageUrls: 0, stylesCached: 0 }
        });
    }
});

// Manual token refresh endpoint
app.post('/api/zoho/refresh-token', async function(req, res) {
    console.log('Manual token refresh requested');
    // Reset cooldown to allow immediate refresh
    lastTokenRefreshAttempt = 0;
    var result = await refreshZohoToken();
    res.json({
        success: result.success,
        error: result.error,
        hasToken: !!zohoAccessToken
    });
});

// ============================================
// WEBHOOK ENDPOINT FOR ZOHO FLOW INTEGRATION
// ============================================
// This endpoint receives Import PO data from Zoho Flow
// Felix can configure Zoho Flow to POST to this URL when POs are created/updated
app.post('/api/webhook/import-po', async function(req, res) {
    console.log('=== WEBHOOK RECEIVED ===');
    console.log('Timestamp:', new Date().toISOString());
    console.log('Body:', JSON.stringify(req.body, null, 2));

    try {
        var data = req.body;

        // Support both single PO and array of POs
        var pos = Array.isArray(data) ? data : [data];
        var imported = 0;
        var errors = [];

        for (var po of pos) {
            try {
                // Map Zoho field names to our database fields (flexible naming)
                var poNumber = po.po_number || po.purchase_order_number || po.PO_Number || po.PONumber || '';
                var vendorName = po.vendor_name || po.vendor || po.Vendor_Name || po.VendorName || po.supplier || po.factory || '';
                var styleNumber = po.style_number || po.style || po.Style_Number || po.StyleNumber || po.sku || '';
                var styleName = po.style_name || po.Style_Name || po.StyleName || po.description || '';
                var commodity = po.commodity || po.Commodity || po.category || po.product_type || '';
                var color = po.color || po.Color || po.colour || '';
                var poQuantity = parseFloat(po.po_quantity || po.quantity || po.Quantity || po.qty || 0) || 0;
                var poUnitPrice = parseFloat(po.po_unit_price || po.unit_price || po.price || po.rate || po.Price || 0) || 0;
                var poTotal = parseFloat(po.po_total || po.po_total_fcy || po.total || po.Total || po.amount || 0) || 0;
                var poWarehouseDate = po.po_warehouse_date || po.warehouse_date || po.eta || po.ETA || po.delivery_date || null;
                var poStatus = po.po_status || po.status || po.Status || 'Open';
                var imageUrl = po.image_url || po.image || po.style_image || '';
                var customer = po.customer || po.Customer || '';

                // Calculate total if not provided but we have qty and price
                if (!poTotal && poQuantity && poUnitPrice) {
                    poTotal = poQuantity * poUnitPrice;
                }

                // Validate required fields
                if (!poNumber || !styleNumber) {
                    errors.push({ po: poNumber || 'unknown', error: 'Missing required field: po_number or style_number' });
                    continue;
                }

                // Parse date if string
                if (poWarehouseDate && typeof poWarehouseDate === 'string') {
                    var parsedDate = new Date(poWarehouseDate);
                    if (!isNaN(parsedDate.getTime())) {
                        poWarehouseDate = parsedDate.toISOString().split('T')[0];
                    } else {
                        poWarehouseDate = null;
                    }
                }

                // Upsert: Update if exists, insert if not (based on po_number + style_number + color)
                var existingRow = await pool.query(
                    'SELECT id FROM order_items WHERE po_number = $1 AND style_number = $2 AND (color = $3 OR (color IS NULL AND $3 IS NULL))',
                    [poNumber, styleNumber, color || null]
                );

                if (existingRow.rows.length > 0) {
                    // Update existing
                    await pool.query(`
                        UPDATE order_items SET
                            vendor_name = COALESCE($1, vendor_name),
                            style_name = COALESCE(NULLIF($2, ''), style_name),
                            commodity = COALESCE(NULLIF($3, ''), commodity),
                            po_quantity = $4,
                            po_unit_price = $5,
                            po_total = $6,
                            po_warehouse_date = COALESCE($7, po_warehouse_date),
                            po_status = COALESCE(NULLIF($8, ''), po_status),
                            image_url = COALESCE(NULLIF($9, ''), image_url),
                            customer = COALESCE(NULLIF($10, ''), customer)
                        WHERE po_number = $11 AND style_number = $12 AND (color = $13 OR (color IS NULL AND $13 IS NULL))
                    `, [vendorName, styleName, commodity, poQuantity, poUnitPrice, poTotal, poWarehouseDate, poStatus, imageUrl, customer, poNumber, styleNumber, color || null]);
                    console.log('Updated PO:', poNumber, styleNumber, color);
                } else {
                    // Insert new - use "IMP-" prefix for so_number since Import POs may not have a sales order
                    var soNumber = 'IMP-' + poNumber;
                    await pool.query(`
                        INSERT INTO order_items (so_number, po_number, vendor_name, style_number, style_name, commodity, color, po_quantity, po_unit_price, po_total, po_warehouse_date, po_status, image_url, customer, status)
                        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, 'Open')
                    `, [soNumber, poNumber, vendorName, styleNumber, styleName, commodity, color, poQuantity, poUnitPrice, poTotal, poWarehouseDate, poStatus, imageUrl, customer]);
                    console.log('Inserted PO:', poNumber, styleNumber, color);
                }

                imported++;
            } catch (rowErr) {
                console.error('Error processing PO:', rowErr);
                errors.push({ po: po.po_number || 'unknown', error: rowErr.message });
            }
        }

        console.log('=== WEBHOOK COMPLETE ===');
        console.log('Imported:', imported, 'Errors:', errors.length);

        res.json({
            success: true,
            message: 'Webhook processed',
            imported: imported,
            errors: errors.length > 0 ? errors : undefined
        });

    } catch (err) {
        console.error('Webhook error:', err);
        res.status(500).json({
            success: false,
            error: err.message
        });
    }
});

// Simple GET endpoint to test webhook is reachable
app.get('/api/webhook/import-po', function(req, res) {
    res.json({
        status: 'ready',
        message: 'Webhook endpoint is active. Send POST requests with Import PO data.',
        expected_fields: {
            required: ['po_number', 'style_number'],
            optional: ['vendor_name', 'style_name', 'commodity', 'color', 'po_quantity', 'po_unit_price', 'po_total', 'po_warehouse_date', 'po_status', 'image_url', 'customer']
        },
        example: {
            po_number: 'PO-12345',
            style_number: '80596J-CB',
            vendor_name: 'ABC Factory',
            commodity: 'Handbags',
            color: 'Black',
            po_quantity: 1000,
            po_unit_price: 12.50,
            po_total: 12500,
            po_warehouse_date: '2025-06-15',
            po_status: 'Open'
        }
    });
});

// =============================================================================
// WEBHOOK: Trigger Sales Orders sync from WorkDrive
// Felix can call this from Zoho Flow when a new CSV is dropped in WorkDrive
// =============================================================================
app.post('/api/webhook/sync-orders', async function(req, res) {
    console.log('=== WEBHOOK: SYNC ORDERS TRIGGERED ===');
    console.log('Timestamp:', new Date().toISOString());
    console.log('Source:', req.headers['user-agent'] || 'unknown');
    if (req.body && Object.keys(req.body).length > 0) {
        console.log('Body:', JSON.stringify(req.body, null, 2));
    }

    try {
        var result = await syncFromWorkDriveFolder(true); // force=true to always sync
        console.log('=== WEBHOOK: SYNC ORDERS COMPLETE ===');
        console.log('Result:', JSON.stringify(result));
        res.json({
            success: true,
            message: 'Sales orders sync triggered via webhook',
            result: result
        });
    } catch (err) {
        console.error('Webhook sync-orders error:', err);
        res.status(500).json({
            success: false,
            error: err.message
        });
    }
});

// Test endpoint to verify webhook is reachable
app.get('/api/webhook/sync-orders', function(req, res) {
    res.json({
        status: 'ready',
        message: 'Sales Orders webhook is active. Send a POST request to trigger a sync from WorkDrive.',
        url: 'https://open-order-production.up.railway.app/api/webhook/sync-orders',
        method: 'POST',
        note: 'No body required. Just POST to this URL to trigger a sync.'
    });
});

// =============================================================================
// WEBHOOK: Trigger Import PO sync from WorkDrive
// Felix can call this from Zoho Flow when a new Import PO CSV is dropped
// =============================================================================
app.post('/api/webhook/sync-import-po', async function(req, res) {
    console.log('=== WEBHOOK: SYNC IMPORT PO TRIGGERED ===');
    console.log('Timestamp:', new Date().toISOString());
    console.log('Source:', req.headers['user-agent'] || 'unknown');
    if (req.body && Object.keys(req.body).length > 0) {
        console.log('Body:', JSON.stringify(req.body, null, 2));
    }

    try {
        var result = await syncImportPOFromWorkDrive(true); // force=true
        console.log('=== WEBHOOK: SYNC IMPORT PO COMPLETE ===');
        console.log('Result:', JSON.stringify(result));
        res.json({
            success: true,
            message: 'Import PO sync triggered via webhook',
            result: result
        });
    } catch (err) {
        console.error('Webhook sync-import-po error:', err);
        res.status(500).json({
            success: false,
            error: err.message
        });
    }
});

// Test endpoint to verify webhook is reachable
app.get('/api/webhook/sync-import-po', function(req, res) {
    res.json({
        status: 'ready',
        message: 'Import PO webhook is active. Send a POST request to trigger a sync from WorkDrive.',
        url: 'https://open-order-production.up.railway.app/api/webhook/sync-import-po',
        method: 'POST',
        note: 'No body required. Just POST to this URL to trigger a sync.'
    });
});

// =============================================================================
// TRIGGER EXPORT: Call Zoho Flow to trigger Analytics export to WorkDrive
// =============================================================================
var ZOHO_FLOW_EXPORT_URL = process.env.ZOHO_FLOW_EXPORT_URL || '';

app.post('/api/trigger-export', async function(req, res) {
    console.log('=== TRIGGER EXPORT REQUESTED ===');
    try {
        if (!ZOHO_FLOW_EXPORT_URL) {
            return res.json({ success: false, error: 'ZOHO_FLOW_EXPORT_URL not configured in environment variables' });
        }
        console.log('Calling Zoho Flow webhook:', ZOHO_FLOW_EXPORT_URL.substring(0, 60) + '...');
        var response = await fetch(ZOHO_FLOW_EXPORT_URL, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ source: 'open-order-app', timestamp: new Date().toISOString() })
        });
        var responseText = await response.text();
        console.log('Zoho Flow response:', response.status, responseText);
        if (response.ok) {
            res.json({ success: true, message: 'Export triggered via Zoho Flow', flowResponse: responseText });
        } else {
            res.json({ success: false, error: 'Zoho Flow returned ' + response.status + ': ' + responseText });
        }
    } catch (err) {
        console.error('Trigger export error:', err);
        res.json({ success: false, error: err.message });
    }
});

// Debug endpoint to check date data
app.get('/api/debug/dates', async function(req, res) {
    try {
        // Check what dates are in the database
        var dateRanges = await pool.query(`
            SELECT
                MIN(delivery_date) as min_date,
                MAX(delivery_date) as max_date,
                COUNT(*) as total_rows
            FROM order_items
        `);

        // Check unique months
        var months = await pool.query(`
            SELECT
                TO_CHAR(delivery_date::date, 'YYYY-MM') as month,
                COUNT(*) as count
            FROM order_items
            WHERE delivery_date IS NOT NULL
            GROUP BY TO_CHAR(delivery_date::date, 'YYYY-MM')
            ORDER BY month
        `);

        // Test FY2026 filter directly
        var fy2026Test = await pool.query(`
            SELECT COUNT(*) as count
            FROM order_items
            WHERE delivery_date::date >= '2025-06-01'::date
            AND delivery_date::date <= '2026-05-31'::date
        `);

        // Check May 2025 specifically
        var may2025 = await pool.query(`
            SELECT COUNT(*) as count
            FROM order_items
            WHERE TO_CHAR(delivery_date::date, 'YYYY-MM') = '2025-05'
        `);

        res.json({
            dateRange: dateRanges.rows[0],
            monthCounts: months.rows,
            fy2026Count: parseInt(fy2026Test.rows[0].count),
            may2025Count: parseInt(may2025.rows[0].count)
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Debug endpoint to test fiscal year filter
app.get('/api/debug/fiscalyear/:fy', async function(req, res) {
    try {
        var fy = req.params.fy;
        var fyInt = parseInt(fy);
        var fyStart = (fyInt - 1) + '-06-01';
        var fyEnd = fyInt + '-05-31';

        // Test the exact filter we use in /api/orders
        var filterTest = await pool.query(
            "SELECT COUNT(*) as total FROM order_items WHERE status IN ('Open', 'Partial') AND delivery_date::date >= $1::date AND delivery_date::date <= $2::date",
            [fyStart, fyEnd]
        );

        // Check if any May of previous year sneaks through
        var may2025Check = await pool.query(
            "SELECT COUNT(*) as may_count FROM order_items WHERE status IN ('Open', 'Partial') AND delivery_date::date >= $1::date AND delivery_date::date <= $2::date AND TO_CHAR(delivery_date, 'YYYY-MM') = $3",
            [fyStart, fyEnd, (fyInt - 1) + '-05']
        );

        // Get the actual months that would be returned
        var monthsReturned = await pool.query(
            "SELECT TO_CHAR(delivery_date, 'YYYY-MM') as month, COUNT(*) as count FROM order_items WHERE status IN ('Open', 'Partial') AND delivery_date::date >= $1::date AND delivery_date::date <= $2::date GROUP BY TO_CHAR(delivery_date, 'YYYY-MM') ORDER BY month",
            [fyStart, fyEnd]
        );

        res.json({
            fiscalYear: fy,
            filterRange: { start: fyStart, end: fyEnd },
            totalRowsMatching: parseInt(filterTest.rows[0].total),
            mayPreviousYearCount: parseInt(may2025Check.rows[0].may_count),
            monthsInResults: monthsReturned.rows
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Debug endpoint to check data totals
app.get('/api/debug/totals', async function(req, res) {
    try {
        // Total records and value
        var totalResult = await pool.query(`
            SELECT COUNT(*) as total_records,
                   SUM(total_amount) as total_dollars,
                   SUM(quantity) as total_units
            FROM order_items
        `);

        // By status
        var statusResult = await pool.query(`
            SELECT status, COUNT(*) as records, SUM(total_amount) as dollars, SUM(quantity) as units
            FROM order_items
            GROUP BY status
        `);

        // Date range in database
        var dateRange = await pool.query(`
            SELECT MIN(delivery_date) as min_date, MAX(delivery_date) as max_date,
                   COUNT(CASE WHEN delivery_date IS NULL THEN 1 END) as null_dates
            FROM order_items
        `);

        // Sample of dates
        var sampleDates = await pool.query(`
            SELECT DISTINCT delivery_date, COUNT(*) as count
            FROM order_items
            WHERE delivery_date IS NOT NULL
            GROUP BY delivery_date
            ORDER BY delivery_date DESC
            LIMIT 20
        `);

        // FY2026 (June 2025 - May 2026)
        var fy2026Result = await pool.query(`
            SELECT COUNT(*) as records, SUM(total_amount) as dollars, SUM(quantity) as units
            FROM order_items
            WHERE delivery_date >= '2025-06-01' AND delivery_date < '2026-06-01'
        `);

        // By customer for FY2026
        var customerFY2026 = await pool.query(`
            SELECT customer, COUNT(*) as records, SUM(total_amount) as dollars, SUM(quantity) as units
            FROM order_items
            WHERE delivery_date >= '2025-06-01' AND delivery_date < '2026-06-01'
            GROUP BY customer
            ORDER BY dollars DESC
        `);

        // Ross customers specifically (ALL data, not just FY2026)
        var rossAllTime = await pool.query(`
            SELECT customer, status, COUNT(*) as records, SUM(total_amount) as dollars
            FROM order_items
            WHERE (customer ILIKE '%ross%' OR customer ILIKE '%dd%forever%')
            GROUP BY customer, status
            ORDER BY customer, status
        `);

        res.json({
            allData: totalResult.rows[0],
            byStatus: statusResult.rows,
            dateRange: dateRange.rows[0],
            sampleDates: sampleDates.rows,
            fy2026: fy2026Result.rows[0],
            customersFY2026: customerFY2026.rows,
            rossCustomersAllTime: rossAllTime.rows
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// Debug endpoint to check a specific style's image
app.get('/api/debug/style/:styleNumber', async function(req, res) {
    try {
        var result = await pool.query(
            'SELECT DISTINCT style_number, style_name, image_url FROM order_items WHERE style_number = $1',
            [req.params.styleNumber]
        );
        if (result.rows.length === 0) {
            return res.json({ error: 'Style not found', styleNumber: req.params.styleNumber });
        }
        var style = result.rows[0];
        var imageUrl = style.image_url;
        var fileId = null;
        if (imageUrl) {
            var match = imageUrl.match(/\/download\/([a-zA-Z0-9]+)/);
            if (match) fileId = match[1];
        }
        // Check if cached
        var cached = false;
        if (fileId) {
            var cacheCheck = await pool.query('SELECT file_id FROM image_cache WHERE file_id = $1', [fileId]);
            cached = cacheCheck.rows.length > 0;
        }
        res.json({
            styleNumber: style.style_number,
            styleName: style.style_name,
            imageUrl: imageUrl || 'NO IMAGE URL IN DATA',
            extractedFileId: fileId || 'COULD NOT EXTRACT',
            cached: cached
        });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// ============================================
// MAIN HTML PAGE
// ============================================
app.get('/', function(req, res) {
    res.send(getHTML());
});

function getHTML() {
    var html = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Mark Edwards - Open Orders Dashboard</title>';
    html += '<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>';
    html += '<style>';

    // Base styles - matching product catalog
    html += '*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display","SF Pro Text",sans-serif;background:#f5f5f7;color:#1e3a5f;font-weight:400;-webkit-font-smoothing:antialiased}';

    // Header
    html += '.header{background:white;padding:0 2rem;height:56px;border-bottom:1px solid rgba(0,0,0,0.06);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100}.header h1{font-size:1.125rem;font-weight:600;color:#1e3a5f;letter-spacing:-0.01em;display:flex;align-items:center;gap:0.5rem}.header h1 span{color:#0088c2}';
    html += '.header-right{display:flex;gap:1rem;align-items:center}';
    html += '.mode-toggle{display:flex;background:#f0f4f8;border-radius:980px;padding:3px;margin-right:1rem}.mode-btn{padding:0.5rem 1rem;border:none;background:transparent;cursor:pointer;font-size:0.8125rem;font-weight:500;border-radius:980px;transition:all 0.2s;color:#6e6e73}.mode-btn.active{background:#1e3a5f;color:white}';

    // Buttons
    html += '.btn{padding:0.625rem 1.25rem;border:none;border-radius:980px;cursor:pointer;font-size:0.875rem;font-weight:500;transition:all 0.2s;letter-spacing:-0.01em}.btn-primary{background:#1e3a5f;color:white}.btn-primary:hover{background:#2a4a6f}.btn-secondary{background:transparent;color:#0088c2;padding:0.5rem 1rem}.btn-secondary:hover{background:rgba(0,136,194,0.08)}.btn-success{background:#0088c2;color:white}.btn-success:hover{background:#007ab8}';

    // Stats bar
    html += '.stats-bar{background:white;padding:1.5rem 2rem;border-bottom:1px solid rgba(0,0,0,0.06);display:flex;gap:3rem;flex-wrap:wrap;align-items:center}';
    html += '.stat-box{}.stat-value{font-size:1.75rem;font-weight:600;color:#1e3a5f;letter-spacing:-0.02em}.stat-label{color:#86868b;font-size:0.75rem;font-weight:500;text-transform:uppercase;letter-spacing:0.02em;margin-top:0.125rem}';
    html += '.stat-box.highlight .stat-value{color:#0088c2}';

    // Filters bar
    html += '.filters-bar{background:white;padding:1rem 2rem;border-bottom:1px solid rgba(0,0,0,0.06);display:flex;gap:0.75rem;flex-wrap:wrap;align-items:center}';
    html += '.filter-group{display:flex;align-items:center;gap:0.5rem}.filter-label{font-size:0.8125rem;color:#86868b;font-weight:500}';
    html += '.filter-select{padding:0.5rem 2rem 0.5rem 1rem;border:1px solid #d2d2d7;border-radius:8px;font-size:0.875rem;background:white;color:#1e3a5f;cursor:pointer;appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns=\'http://www.w3.org/2000/svg\' width=\'12\' height=\'12\' viewBox=\'0 0 12 12\'%3E%3Cpath fill=\'%2386868b\' d=\'M6 8L2 4h8z\'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 0.75rem center}.filter-select:focus{outline:none;border-color:#0088c2}';
    html += '.filter-select.active{border-color:#0088c2;background-color:rgba(0,136,194,0.05)}';
    html += '.status-toggle{display:flex;background:#f0f4f8;border-radius:980px;padding:3px}.status-btn{padding:0.5rem 1.25rem;border:none;background:transparent;cursor:pointer;font-size:0.8125rem;font-weight:500;border-radius:980px;transition:all 0.2s;color:#6e6e73}';
    html += '.status-btn.active{color:white}.status-btn[data-status="Open"].active{background:#34c759}.status-btn[data-status="Invoiced"].active{background:#8e8e93}.status-btn[data-status="Received"].active{background:#8e8e93}.status-btn[data-status="All"].active{background:#0088c2}';
    html += '.clear-filters{padding:0.5rem 1rem;border:none;background:transparent;color:#ff3b30;cursor:pointer;font-size:0.8125rem;font-weight:500;border-radius:6px}.clear-filters:hover{background:rgba(255,59,48,0.1)}';
    html += '.view-toggle{display:flex;background:#f0f4f8;border-radius:980px;padding:3px;margin-left:auto}.view-btn{padding:0.5rem 1rem;border:none;background:transparent;cursor:pointer;font-size:0.8125rem;font-weight:500;border-radius:980px;transition:all 0.2s;color:#6e6e73}.view-btn.active{background:#1e3a5f;color:white}';

    // Main content
    html += '.main{padding:1.5rem 2rem;max-width:1800px;margin:0 auto}';

    // Product grid
    html += '.product-grid{display:grid;gap:1.25rem;grid-template-columns:repeat(auto-fill,minmax(280px,1fr))}';

    // Timeline bar at top
    html += '.timeline-bar{background:white;padding:1rem 2rem;border-bottom:1px solid rgba(0,0,0,0.06);display:flex;gap:0.5rem;overflow-x:auto;margin-bottom:1rem;border-radius:12px}';
    html += '.timeline-month{padding:0.5rem 1rem;border-radius:8px;cursor:pointer;white-space:nowrap;font-size:0.8125rem;font-weight:500;transition:all 0.2s;border:1px solid #e0e0e0;background:white;color:#6e6e73}';
    html += '.timeline-month:hover{border-color:#0088c2;color:#0088c2}';
    html += '.timeline-month.active{background:#1e3a5f;color:white;border-color:#1e3a5f}';
    html += '.timeline-month .tm-amounts{display:flex;gap:0.5rem;font-size:0.625rem;font-weight:600;margin-top:0.25rem}';
    html += '.timeline-month .tm-open{color:#34c759}.timeline-month .tm-invoiced{color:#8e8e93}.timeline-month .tm-total{color:#0088c2}';
    html += '.timeline-month.active .tm-open{color:#86efac}.timeline-month.active .tm-invoiced{color:#d1d5db}.timeline-month.active .tm-total{color:#7dd3fc}';

    // Month section - redesigned with full-width header
    html += '.month-section{margin-bottom:0.5rem}';
    html += '.month-header{background:linear-gradient(135deg,#1e3a5f 0%,#2d5a87 100%);color:white;padding:1.25rem 1.5rem;border-radius:12px 12px 0 0;display:flex;justify-content:space-between;align-items:center;cursor:pointer;margin-top:1rem}';
    html += '.month-header:first-child{margin-top:0}';
    html += '.month-header:hover{background:linear-gradient(135deg,#2d5a87 0%,#3d6a97 100%)}';
    html += '.month-header h2{font-size:1.375rem;font-weight:700;color:white;margin:0;display:flex;align-items:center;gap:0.75rem}';
    html += '.month-header h2 .collapse-icon{font-size:0.875rem;transition:transform 0.3s}';
    html += '.month-header.collapsed h2 .collapse-icon{transform:rotate(-90deg)}';
    html += '.month-stats{display:flex;gap:2rem;font-size:0.9375rem}.month-stats span{font-weight:500;color:rgba(255,255,255,0.8)}.month-stats .money{color:#7dd3fc;font-weight:700;font-size:1.125rem}';
    html += '.month-body{background:white;border:1px solid #e0e0e0;border-top:none;border-radius:0 0 12px 12px;padding:1.25rem;margin-bottom:1rem}';
    html += '.month-body.collapsed{display:none}';

    // Commodity section - collapsible with color coding
    html += '.commodity-colors{--comm-top:#e8f4fc;--comm-dress:#fce8f4;--comm-bottom:#e8fcf4;--comm-jacket:#fcf4e8;--comm-sweater:#f4e8fc;--comm-other:#f5f5f7}';
    html += '.commodity-section{margin-bottom:1rem;border-radius:10px;overflow:hidden;border:1px solid #e8e8e8}';
    html += '.commodity-section:last-child{margin-bottom:0}';
    html += '.commodity-header{padding:0.875rem 1rem;cursor:pointer;display:flex;justify-content:space-between;align-items:center;transition:all 0.2s}';
    html += '.commodity-header:hover{filter:brightness(0.97)}';
    html += '.commodity-header.comm-top{background:var(--comm-top)}.commodity-header.comm-dress{background:var(--comm-dress)}.commodity-header.comm-bottom{background:var(--comm-bottom)}.commodity-header.comm-jacket{background:var(--comm-jacket)}.commodity-header.comm-sweater{background:var(--comm-sweater)}.commodity-header.comm-other{background:var(--comm-other)}';
    html += '.commodity-header-left{display:flex;align-items:center;gap:0.75rem}';
    html += '.commodity-header-left h3{font-size:0.9375rem;font-weight:600;color:#1e3a5f;margin:0;text-transform:uppercase;letter-spacing:0.03em}';
    html += '.commodity-header-left .collapse-icon{font-size:0.75rem;color:#86868b;transition:transform 0.3s}';
    html += '.commodity-header.collapsed .collapse-icon{transform:rotate(-90deg)}';
    html += '.commodity-header-right{display:flex;gap:1.5rem;font-size:0.8125rem;color:#6e6e73}';
    html += '.commodity-header-right .comm-stat{font-weight:500}.commodity-header-right .comm-money{color:#0088c2;font-weight:600}';
    html += '.commodity-body{padding:1rem;background:white}';
    html += '.commodity-body.collapsed{display:none}';
    html += '.commodity-progress{height:4px;background:#e0e0e0;border-radius:2px;margin-top:0.5rem;overflow:hidden}';
    html += '.commodity-progress-fill{height:100%;background:#0088c2;border-radius:2px}';

    // Multi-select dropdown
    html += '.multi-select{position:relative;min-width:140px}.multi-select-display{background:white;border:1px solid #d2d2d7;border-radius:8px;padding:0.5rem 0.75rem;cursor:pointer;display:flex;justify-content:space-between;align-items:center;font-size:0.875rem}.multi-select-display:hover{border-color:#0088c2}.multi-select-display.active{border-color:#0088c2;background:#e8f4fc}';
    html += '.multi-select-arrow{font-size:0.625rem;color:#86868b;margin-left:0.5rem}';
    html += '.multi-select-dropdown{position:absolute;top:100%;left:0;right:0;background:white;border:1px solid #d2d2d7;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.15);max-height:350px;overflow-y:auto;display:none;z-index:100;min-width:200px}';
    html += '.multi-select-dropdown.open{display:block}';
    html += '.multi-select-search{padding:0.5rem;border-bottom:1px solid #e0e0e0;position:sticky;top:0;background:white}';
    html += '.multi-select-search input{width:100%;padding:0.5rem;border:1px solid #d2d2d7;border-radius:6px;font-size:0.8125rem;outline:none}.multi-select-search input:focus{border-color:#0088c2}';
    html += '.multi-select-options{max-height:250px;overflow-y:auto}';
    html += '.multi-select-option{padding:0.5rem 0.75rem;cursor:pointer;display:flex;align-items:center;gap:0.5rem;font-size:0.875rem}.multi-select-option:hover{background:#f5f5f7}.multi-select-option.hidden{display:none}';
    html += '.multi-select-option input{margin:0}';
    html += '.multi-select-actions{padding:0.5rem;border-bottom:1px solid #e0e0e0;display:flex;gap:0.5rem;position:sticky;top:41px;background:white}.multi-select-actions button{flex:1;padding:0.375rem;font-size:0.75rem;border:none;border-radius:4px;cursor:pointer}.multi-select-actions .select-all{background:#e8f4fc;color:#0088c2}.multi-select-actions .clear-all{background:#f5f5f7;color:#86868b}';

    // Charts
    html += '.charts-container{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:1.5rem}';
    html += '.chart-card{background:white;border-radius:16px;padding:1.5rem;border:1px solid rgba(0,0,0,0.04)}';
    html += '.chart-card h3{font-size:1rem;font-weight:600;color:#1e3a5f;margin:0 0 1rem 0}';
    html += '.chart-wrapper{height:300px;position:relative}';
    html += '.export-btn{background:#0088c2;color:white;border:none;padding:0.625rem 1.25rem;border-radius:8px;font-size:0.875rem;font-weight:500;cursor:pointer;display:flex;align-items:center;gap:0.5rem;margin-bottom:1rem}.export-btn:hover{background:#006699}';

    // Dashboard hybrid view
    html += '.dashboard-layout{display:grid;grid-template-columns:380px 1fr;gap:1.5rem;align-items:start;position:relative;transition:grid-template-columns 0.3s}';
    html += '.dashboard-layout.sidebar-collapsed{grid-template-columns:0 1fr}';
    html += '.dashboard-layout.sidebar-collapsed .dashboard-charts{opacity:0;pointer-events:none;overflow:hidden;width:0;padding:0;margin:0}';
    html += '@media(max-width:1200px){.dashboard-layout{grid-template-columns:1fr}}';
    html += '.sidebar-toggle{position:fixed;left:8px;top:50%;transform:translateY(-50%);background:#1e3a5f;color:white;border:none;padding:10px 8px;border-radius:6px;font-size:0.75rem;cursor:pointer;z-index:100;display:none;box-shadow:0 2px 8px rgba(0,0,0,0.15)}';
    html += '.sidebar-toggle:hover{background:#0088c2}';
    html += '.dashboard-layout.sidebar-collapsed .sidebar-toggle{display:block}';
    html += '.sidebar-hide-link{float:right;font-size:0.7rem;color:#86868b;cursor:pointer;font-weight:normal}';
    html += '.sidebar-hide-link:hover{color:#0088c2}';
    html += '.dashboard-charts{display:flex;flex-direction:column;gap:1rem;transition:opacity 0.3s,width 0.3s;position:relative}';
    html += '.dashboard-card{background:white;border-radius:16px;padding:1rem;border:1px solid rgba(0,0,0,0.04)}';
    html += '.dashboard-card h3{font-size:0.9375rem;font-weight:600;color:#1e3a5f;margin:0 0 0.75rem 0}';
    // Mini stacked bar chart - scrollable with arrows
    html += '.mini-stacked-container{position:relative}';
    html += '.mini-stacked-wrapper{overflow-x:auto;overflow-y:hidden;padding-bottom:8px;scroll-behavior:smooth;margin:0 24px}';
    html += '.mini-stacked-wrapper::-webkit-scrollbar{height:8px}';
    html += '.mini-stacked-wrapper::-webkit-scrollbar-track{background:#e0e0e0;border-radius:4px}';
    html += '.mini-stacked-wrapper::-webkit-scrollbar-thumb{background:#0088c2;border-radius:4px}';
    html += '.mini-stacked-wrapper::-webkit-scrollbar-thumb:hover{background:#006a99}';
    html += '.mini-stacked-scroll-btn{position:absolute;top:50%;transform:translateY(-50%);width:20px;height:40px;background:#1e3a5f;color:white;border:none;border-radius:4px;cursor:pointer;font-size:12px;z-index:5;display:flex;align-items:center;justify-content:center}';
    html += '.mini-stacked-scroll-btn:hover{background:#0088c2}';
    html += '.mini-stacked-scroll-btn.left{left:0}';
    html += '.mini-stacked-scroll-btn.right{right:0}';
    html += '.mini-stacked{display:flex;gap:4px;align-items:flex-end;height:120px;min-width:max-content}';
    html += '.mini-stacked-bar{width:40px;min-width:40px;display:flex;flex-direction:column;justify-content:flex-end;cursor:pointer;transition:opacity 0.15s}';
    html += '.mini-stacked-bar:hover{opacity:0.85}';
    html += '.mini-stacked-segment{transition:height 0.3s}';
    html += '.mini-stacked-label{font-size:0.55rem;color:#666;text-align:center;margin-top:4px;white-space:nowrap}';
    html += '.mini-stacked-legend{display:flex;flex-wrap:wrap;gap:6px;margin-top:8px;padding-top:8px;border-top:1px solid #eee}';
    html += '.legend-item{display:flex;align-items:center;gap:3px;font-size:0.65rem;color:#666;cursor:pointer;padding:2px 4px;border-radius:3px}';
    html += '.legend-item:hover{background:#f0f4f8}';
    html += '.legend-color{width:10px;height:10px;border-radius:2px}';
    html += '.dashboard-treemap{display:flex;flex-wrap:wrap;gap:3px}';
    html += '.treemap-item{padding:6px 8px;color:white;border-radius:6px;cursor:pointer;transition:all 0.15s;min-width:55px;box-sizing:border-box}';
    html += '.treemap-item:hover{opacity:0.85;transform:scale(1.02)}';
    html += '.treemap-label{font-weight:600;font-size:0.6875rem;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
    html += '.treemap-value{font-size:0.6875rem;opacity:0.95}';
    html += '.treemap-pct{font-size:0.5625rem;opacity:0.8}';
    html += '.treemap-header{display:flex;align-items:center;gap:0.5rem;margin-bottom:0.5rem;flex-wrap:wrap}';
    html += '.treemap-header h3{margin:0;flex-shrink:0}';
    html += '.treemap-toggle{display:flex;background:#f0f0f0;border-radius:6px;padding:2px;margin-left:auto}';
    html += '.treemap-toggle button{padding:0.25rem 0.5rem;border:none;background:transparent;font-size:0.6875rem;color:#86868b;cursor:pointer;border-radius:4px;transition:all 0.2s}';
    html += '.treemap-toggle button.active{background:#0088c2;color:white}';
    html += '.treemap-toggle button:hover:not(.active){background:#e5e5e5}';
    html += '.dashboard-customers{display:flex;flex-direction:column;gap:6px}';
    html += '.dashboard-customers-scroll{display:flex;flex-direction:column;gap:4px;max-height:300px;overflow-y:auto}';
    html += '.customer-bar{display:grid;grid-template-columns:1fr auto;gap:0.5rem;align-items:center;padding:6px 8px;border-radius:6px;cursor:pointer;position:relative;background:#f5f5f7}';
    html += '.customer-bar:hover{background:#e8f4fc}';
    html += '.customer-name{font-size:0.75rem;font-weight:500;color:#1e3a5f;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;z-index:1}';
    html += '.customer-bar-fill{position:absolute;left:0;top:0;bottom:0;border-radius:6px;opacity:0.2}';
    html += '.customer-value{font-size:0.75rem;font-weight:600;color:#0088c2;z-index:1}';
    html += '.dashboard-products{background:white;border-radius:16px;padding:1rem;border:1px solid rgba(0,0,0,0.04)}';
    html += '.dashboard-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:0.75rem}';
    html += '.dashboard-style-card{background:#f9fafb;border-radius:10px;overflow:hidden;cursor:pointer;transition:transform 0.15s,box-shadow 0.15s}';
    html += '.dashboard-style-card:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,0.1)}';
    html += '.dashboard-style-img{height:120px;background:#f0f4f8;display:flex;align-items:center;justify-content:center}';
    html += '.dashboard-style-img img{max-width:100%;max-height:100%;object-fit:contain}';
    html += '.dashboard-style-info{padding:0.625rem}';
    html += '.dashboard-style-name{font-size:0.75rem;font-weight:600;color:#1e3a5f;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
    html += '.dashboard-style-num{font-size:0.6875rem;color:#86868b}';
    html += '.dashboard-style-comm{font-size:0.625rem;color:#0088c2;margin-top:2px}';
    html += '.dashboard-style-stats{display:flex;justify-content:space-between;margin-top:4px;font-size:0.6875rem;color:#666}';
    html += '.dashboard-style-stats .money{color:#34c759;font-weight:600}';
    // Dashboard timeline - ultra-compact single row
    html += '.dashboard-timeline{background:white;border-radius:8px;padding:0.35rem 0.6rem;margin-bottom:0.5rem;border:1px solid rgba(0,0,0,0.04);display:flex;align-items:center;gap:0.5rem}';
    html += '.timeline-title{font-weight:600;color:#1e3a5f;font-size:0.65rem;white-space:nowrap}';
    html += '.timeline-bars{display:flex;gap:2px;flex:1;align-items:center}';
    html += '.timeline-month{cursor:pointer;transition:all 0.15s}';
    html += '.timeline-month:hover{transform:scale(1.05)}';
    html += '.timeline-month.active .timeline-bar{background:#1e3a5f}';
    html += '.timeline-bar{background:#0088c2;border-radius:3px;padding:2px 4px;text-align:center;min-width:24px}';
    html += '.bar-month{color:white;font-weight:600;font-size:0.55rem}';
    html += '.bar-year{color:rgba(255,255,255,0.8);font-size:0.5rem}';
    html += '.timeline-stats{font-size:0.45rem;color:#888;text-align:center;margin-top:1px;line-height:1}';
    html += '.timeline-dollars{color:#1e3a5f;font-weight:600}';
    html += '.timeline-units{color:#86868b;font-size:0.45rem}';
    html += '.timeline-clear{background:#ff3b30;color:white;border:none;padding:2px 6px;border-radius:3px;font-size:0.55rem;cursor:pointer;margin-left:4px}';
    html += '.timeline-clear:hover{background:#d63030}';
    html += '.timeline-year-group{display:flex;align-items:center;margin-left:6px;gap:1px}';
    html += '.timeline-year-group:first-child{margin-left:0}';
    html += '.timeline-year-label{background:#6b7280;color:white;padding:1px 4px;font-size:0.5rem;font-weight:700;border-radius:3px;margin-right:2px;writing-mode:horizontal-tb}';
    html += '.timeline-year-months{display:flex;gap:1px}';
    html += '.filter-clear-btn{background:#ff3b30;color:white;border:none;padding:0.5rem 0.75rem;border-radius:6px;font-size:0.75rem;cursor:pointer;margin-top:0.5rem;width:100%;font-weight:500}';
    html += '.filter-clear-btn:hover{background:#d63030}';
    // YoY comparison styles
    html += '.treemap-yoy{font-size:0.6875rem;font-weight:700;margin-top:2px}';
    html += '.yoy-list{display:flex;flex-direction:column;gap:6px}';
    html += '.yoy-row{display:flex;justify-content:space-between;align-items:center;padding:8px;background:#f8fafc;border-radius:6px}';
    html += '.yoy-cust{font-weight:600;color:#1e3a5f;font-size:0.8125rem;flex:1;min-width:0;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';
    html += '.yoy-values{display:flex;align-items:center;gap:6px;font-size:0.75rem}';
    html += '.yoy-ty{font-weight:600;color:#1e3a5f}';
    html += '.yoy-vs{color:#86868b;font-size:0.625rem}';
    html += '.yoy-ly{color:#86868b}';
    html += '.yoy-up{color:#34c759;font-weight:700;font-size:0.6875rem}';
    html += '.yoy-down{color:#ff3b30;font-weight:700;font-size:0.6875rem}';
    html += '.yoy-flat{color:#86868b;font-weight:700;font-size:0.6875rem}';
    // Missing customers styles
    html += '.missing-customers{border:2px solid #ff3b30;background:#fff5f5}';
    html += '.missing-list{display:flex;flex-direction:column;gap:4px}';
    html += '.missing-item{display:flex;justify-content:space-between;align-items:center;padding:6px 8px;background:white;border-radius:4px;font-size:0.75rem}';
    html += '.missing-name{font-weight:600;color:#1e3a5f}';
    html += '.missing-details{color:#86868b;font-size:0.6875rem}';
    // Heat badge styles
    html += '.heat-badge{position:absolute;top:4px;right:4px;font-size:1rem;z-index:1}';
    html += '.dashboard-style-card{position:relative}';

    // Group by customer toggle and styles
    html += '.group-toggle-btn{padding:0.5rem 1rem;border-radius:8px;border:1px solid #e5e5e5;background:white;font-size:0.8125rem;color:#1e3a5f;cursor:pointer;transition:all 0.2s}';
    html += '.group-toggle-btn:hover{background:#f5f5f7;border-color:#0088c2}';
    html += '.group-toggle-btn.active{background:#0088c2;color:white;border-color:#0088c2}';
    html += '.customer-group{margin-bottom:1.5rem;background:white;border-radius:12px;border:1px solid #e5e5e5;overflow:hidden}';
    html += '.customer-group-header{padding:1rem 1.25rem;background:linear-gradient(135deg,#1e3a5f 0%,#2d5a87 100%);color:white;display:flex;justify-content:space-between;align-items:center;cursor:pointer}';
    html += '.customer-group-header:hover{background:linear-gradient(135deg,#2d5a87 0%,#0088c2 100%)}';
    html += '.customer-group-name{font-weight:600;font-size:1rem}';
    html += '.customer-group-stats{font-size:0.8125rem;opacity:0.9}';
    html += '.customer-group-stats .money{color:#4da6d9;font-weight:600}';
    html += '.customer-group-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(100px,1fr));gap:0.5rem;padding:1rem}';
    html += '.mini-style-card{background:#f9fafb;border-radius:8px;overflow:hidden;cursor:pointer;transition:transform 0.15s,box-shadow 0.15s}';
    html += '.mini-style-card:hover{transform:translateY(-2px);box-shadow:0 4px 12px rgba(0,0,0,0.1)}';
    html += '.mini-style-img{width:100%;aspect-ratio:1;background:#f0f0f0;display:flex;align-items:center;justify-content:center;overflow:hidden}';
    html += '.mini-style-img img{width:100%;height:100%;object-fit:cover}';
    html += '.mini-style-info{padding:0.5rem;text-align:center}';
    html += '.mini-style-num{font-size:0.6875rem;font-weight:600;color:#1e3a5f;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
    html += '.mini-style-value{font-size:0.625rem;color:#0088c2;font-weight:500}';

    // Stacked tiles styles
    html += '.style-stack{position:relative;cursor:pointer}';
    html += '.style-stack .dashboard-style-card{position:relative;z-index:3}';
    html += '.style-stack::before{content:"";position:absolute;top:4px;left:4px;right:-4px;bottom:-4px;background:#e0e0e0;border-radius:10px;z-index:1}';
    html += '.style-stack::after{content:"";position:absolute;top:8px;left:8px;right:-8px;bottom:-8px;background:#c8c8c8;border-radius:10px;z-index:0}';
    html += '.style-stack .stack-badge{position:absolute;top:-8px;right:-8px;background:#0088c2;color:white;font-size:0.65rem;font-weight:700;padding:2px 6px;border-radius:10px;z-index:10;box-shadow:0 2px 4px rgba(0,0,0,0.2)}';
    html += '.style-stack:hover{transform:translateY(-2px)}';
    html += '.style-stack.expanded{z-index:100}';
    html += '.stack-expanded-overlay{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);z-index:99;display:none}';
    html += '.stack-expanded-overlay.visible{display:block}';
    html += '.stack-expanded-container{position:absolute;top:0;left:0;display:none;flex-wrap:wrap;gap:0.5rem;background:white;padding:1rem;border-radius:12px;box-shadow:0 8px 32px rgba(0,0,0,0.2);z-index:101;min-width:400px;max-width:600px}';
    html += '.style-stack.expanded .stack-expanded-container{display:flex}';
    html += '.stack-expanded-container .dashboard-style-card{flex:0 0 calc(50% - 0.25rem);margin:0}';
    html += '.stack-expanded-header{width:100%;display:flex;justify-content:space-between;align-items:center;padding-bottom:0.75rem;border-bottom:1px solid #eee;margin-bottom:0.5rem}';
    html += '.stack-expanded-title{font-weight:600;color:#1e3a5f;font-size:0.9rem}';
    html += '.stack-expanded-close{background:none;border:none;font-size:1.25rem;cursor:pointer;color:#86868b;padding:0 0.25rem}';
    html += '.stack-expanded-close:hover{color:#1e3a5f}';

    // Summary matrix table
    html += '.summary-container{background:white;border-radius:16px;padding:1.5rem;overflow-x:auto}';
    html += '.summary-table{width:100%;border-collapse:collapse;font-size:0.8125rem}';
    html += '.summary-table th,.summary-table td{padding:0.75rem 1rem;text-align:right;border-bottom:1px solid #f0f0f0}';
    html += '.summary-table th{background:#f5f5f7;font-weight:600;color:#1e3a5f;position:sticky;top:0}';
    html += '.summary-table th:first-child,.summary-table td:first-child{text-align:left;font-weight:600;position:sticky;left:0;background:white;z-index:1}';
    html += '.summary-table th:first-child{background:#f5f5f7;z-index:2}';
    html += '.summary-table tr:hover td{background:#f9fafb}';
    html += '.summary-table tr:hover td:first-child{background:#f9fafb}';
    html += '.summary-table .row-total{background:#e8f4fc !important;font-weight:600;color:#0088c2}';
    html += '.summary-table .col-total{background:#f5f5f7;font-weight:600}';
    html += '.summary-table .grand-total{background:#1e3a5f !important;color:white;font-weight:700}';
    html += '.summary-cell{display:flex;flex-direction:column;align-items:flex-end;gap:2px}';
    html += '.summary-cell .dollars{font-weight:600;color:#1e3a5f}';
    html += '.summary-cell .units{font-size:0.6875rem;color:#86868b}';
    html += '.summary-cell.has-value .dollars{color:#0088c2}';
    html += '.summary-legend{display:flex;gap:2rem;margin-bottom:1rem;font-size:0.8125rem;color:#86868b}';
    html += '.summary-legend span{display:flex;align-items:center;gap:0.5rem}';
    html += '.comm-color-box{width:12px;height:12px;border-radius:3px}';
    html += '.summary-table .comm-row-top td:first-child{border-left:4px solid #0088c2}';
    html += '.summary-table .comm-row-bottom td:first-child{border-left:4px solid #34c759}';
    html += '.summary-table .comm-row-dress td:first-child{border-left:4px solid #af52de}';
    html += '.summary-table .comm-row-sweater td:first-child{border-left:4px solid #ff9500}';
    html += '.summary-table .comm-row-jacket td:first-child{border-left:4px solid #ff3b30}';
    html += '.summary-table .comm-row-other td:first-child{border-left:4px solid #86868b}';
    // Summary toggle and expandable rows
    html += '.summary-header{display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem}';
    html += '.summary-toggle{display:flex;align-items:center;gap:0.5rem;font-size:0.875rem;color:#1e3a5f}';
    html += '.summary-toggle-btns{display:flex;background:#f0f0f0;border-radius:8px;padding:2px}';
    html += '.summary-toggle-btn{padding:0.375rem 0.75rem;border-radius:6px;border:none;background:transparent;font-size:0.8125rem;color:#86868b;cursor:pointer;transition:all 0.2s}';
    html += '.summary-toggle-btn.active{background:#0088c2;color:white;font-weight:500}';
    html += '.expand-btn{background:none;border:none;cursor:pointer;font-size:0.75rem;color:#86868b;padding:0.25rem;margin-right:0.25rem;transition:transform 0.2s}';
    html += '.expand-btn.expanded{transform:rotate(90deg)}';
    html += '.summary-subrow{background:#fafafa}';
    html += '.summary-subrow td{font-size:0.75rem !important;padding:0.5rem 1rem !important}';
    html += '.summary-subrow td:first-child{padding-left:2rem !important;font-weight:400 !important;color:#666}';

    // Style card
    html += '.style-card{background:white;border-radius:16px;overflow:hidden;cursor:pointer;transition:all 0.3s ease;border:1px solid rgba(0,0,0,0.04)}.style-card:hover{transform:translateY(-4px);box-shadow:0 12px 40px rgba(0,0,0,0.12)}';
    html += '.style-image{height:200px;background:#f5f5f7;display:flex;align-items:center;justify-content:center;overflow:hidden;position:relative}.style-image img{max-width:90%;max-height:90%;object-fit:contain}';
    html += '.img-retry-btn{background:#0088c2;color:white;border:none;padding:0.5rem 1rem;border-radius:8px;cursor:pointer;font-size:0.75rem;display:flex;align-items:center;gap:0.35rem;transition:all 0.2s}.img-retry-btn:hover{background:#006699;transform:scale(1.05)}.img-retry-btn.loading{opacity:0.7;pointer-events:none}';
    html += '.style-badge{position:absolute;top:12px;right:12px;background:#0088c2;color:white;padding:0.25rem 0.75rem;border-radius:980px;font-size:0.6875rem;font-weight:600}';
    html += '.commodity-badge{position:absolute;top:12px;left:12px;background:rgba(30,58,95,0.9);color:white;padding:0.25rem 0.625rem;border-radius:6px;font-size:0.625rem;font-weight:500;text-transform:uppercase;letter-spacing:0.02em}';
    html += '.gm-badge{position:absolute;bottom:12px;right:12px;padding:0.25rem 0.5rem;border-radius:6px;font-size:0.6875rem;font-weight:600;background:rgba(255,255,255,0.9)}';
    html += '.gm-badge.gm-high{color:#34c759}.gm-badge.gm-mid{color:#ff9500}.gm-badge.gm-low{color:#ff3b30}.gm-badge.gm-none{color:#8e8e93}';
    html += '.style-info{padding:1rem 1.25rem}.style-number{font-size:0.6875rem;color:#86868b;text-transform:uppercase;letter-spacing:0.05em;font-weight:500}.style-name{font-size:0.9375rem;font-weight:600;margin:0.25rem 0;color:#1e3a5f;letter-spacing:-0.01em;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
    html += '.style-stats{display:flex;justify-content:space-between;margin-top:0.75rem;padding-top:0.75rem;border-top:1px solid #f0f0f0}.style-stat{text-align:center}.style-stat-value{font-size:1rem;font-weight:600;color:#1e3a5f}.style-stat-value.money::before{content:"$"}.style-stat-label{font-size:0.625rem;color:#86868b;text-transform:uppercase;margin-top:0.125rem}';
    html += '.order-count{font-size:0.75rem;color:#86868b;margin-top:0.5rem}';

    // Sales Order view
    html += '.so-grid{display:flex;flex-direction:column;gap:1rem}';
    html += '.so-card{background:white;border-radius:16px;overflow:hidden;border:1px solid rgba(0,0,0,0.04)}.so-header{padding:1.25rem;display:flex;justify-content:space-between;align-items:center;border-bottom:1px solid #f0f0f0;cursor:pointer}.so-header:hover{background:#fafafa}';
    html += '.so-info h3{font-size:1rem;font-weight:600;color:#1e3a5f;margin-bottom:0.25rem}.so-info span{font-size:0.8125rem;color:#86868b}';
    html += '.so-meta{text-align:right}.so-meta .delivery{font-size:0.875rem;font-weight:600;color:#0088c2}.so-meta .total{font-size:1.125rem;font-weight:600;color:#1e3a5f}';
    html += '.so-items{padding:1rem;display:none;background:#fafafa}.so-items.expanded{display:block}';
    html += '.so-item{display:flex;align-items:center;gap:1rem;padding:0.75rem;background:white;border-radius:8px;margin-bottom:0.5rem}.so-item:last-child{margin-bottom:0}';
    html += '.so-item-image{width:60px;height:60px;background:#f5f5f7;border-radius:6px;overflow:hidden;display:flex;align-items:center;justify-content:center}.so-item-image img{max-width:100%;max-height:100%;object-fit:contain}';
    html += '.so-item-info{flex:1}.so-item-style{font-weight:600;color:#1e3a5f;font-size:0.875rem}.so-item-details{font-size:0.75rem;color:#86868b}';
    html += '.so-item-qty{text-align:right}.so-item-qty .qty{font-weight:600;color:#1e3a5f}.so-item-qty .amount{font-size:0.8125rem;color:#86868b}';

    // Empty state
    html += '.empty-state{text-align:center;padding:4rem 2rem;color:#86868b}.empty-state h3{font-size:1.25rem;font-weight:600;color:#1e3a5f;margin-bottom:0.5rem}.empty-state p{font-size:0.9375rem}';

    // Modal
    html += '.modal{position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.5);display:none;align-items:center;justify-content:center;z-index:1000;padding:2rem}.modal.active{display:flex}';
    html += '.modal-content{background:white;border-radius:18px;max-width:900px;width:100%;max-height:90vh;overflow:auto;position:relative}';
    html += '.modal-close{position:absolute;top:1rem;right:1rem;background:rgba(0,0,0,0.06);border:none;font-size:1.25rem;cursor:pointer;border-radius:50%;width:32px;height:32px;z-index:10;color:#1e3a5f}.modal-close:hover{background:rgba(0,0,0,0.1)}';
    html += '.modal-body{display:flex;flex-direction:column}';
    html += '.modal-image{height:300px;background:#f5f5f7;display:flex;align-items:center;justify-content:center}.modal-image img{max-width:90%;max-height:90%;object-fit:contain}';
    html += '.modal-details{padding:1.5rem}';
    html += '.modal-header{margin-bottom:1.5rem}.modal-header h2{font-size:1.5rem;font-weight:600;color:#1e3a5f;margin-bottom:0.25rem}.modal-header p{color:#86868b;font-size:0.875rem}';
    html += '.modal-stats{display:flex;gap:2rem;margin-bottom:1.5rem;padding:1rem;background:#f5f5f7;border-radius:12px}.modal-stat{text-align:center;flex:1}.modal-stat-value{font-size:1.5rem;font-weight:600;color:#1e3a5f}.modal-stat-value.money::before{content:"$"}.modal-stat-label{font-size:0.75rem;color:#86868b;text-transform:uppercase}';
    html += '.orders-list{max-height:300px;overflow-y:auto}.orders-list h4{font-size:0.875rem;font-weight:600;color:#86868b;text-transform:uppercase;letter-spacing:0.02em;margin-bottom:0.75rem}';
    html += '.order-row{display:flex;justify-content:space-between;align-items:center;padding:0.75rem;border-radius:8px;margin-bottom:0.5rem;background:#f5f5f7}.order-row:hover{background:#ebebed}';
    html += '.order-row-left{}.order-row-so{font-weight:600;color:#1e3a5f;font-size:0.875rem}.order-row-customer{font-size:0.75rem;color:#86868b}.order-row-right{text-align:right}.order-row-qty{font-weight:600;color:#1e3a5f}.order-row-date{font-size:0.75rem;color:#0088c2}';

    // Upload modal
    html += '.upload-modal .modal-content{max-width:500px;padding:2rem}';
    html += '.upload-area{border:2px dashed #d2d2d7;padding:3rem;text-align:center;border-radius:12px;margin:1.5rem 0;cursor:pointer;transition:all 0.2s}.upload-area:hover{border-color:#0088c2;background:rgba(0,136,194,0.02)}.upload-area.dragover{border-color:#0088c2;background:rgba(0,136,194,0.05)}';
    html += '.upload-area input{display:none}.upload-icon{font-size:3rem;margin-bottom:1rem}.upload-text{color:#86868b;font-size:0.9375rem}.upload-text strong{color:#0088c2}';
    html += '.upload-progress{margin-top:1rem;display:none}.upload-progress.active{display:block}.progress-bar{height:8px;background:#f0f0f0;border-radius:4px;overflow:hidden}.progress-fill{height:100%;background:#0088c2;transition:width 0.3s}';

    // Chat bubble and panel
    html += '.chat-bubble{position:fixed;bottom:24px;right:24px;background:#1e3a5f;border-radius:28px;display:flex;align-items:center;gap:0.5rem;padding:0.75rem 1.25rem;cursor:pointer;box-shadow:0 4px 16px rgba(0,0,0,0.16);z-index:999;transition:all 0.3s}.chat-bubble:hover{transform:scale(1.05);box-shadow:0 6px 24px rgba(0,0,0,0.2)}.chat-bubble svg{width:24px;height:24px;fill:white}.chat-bubble-label{color:white;font-size:0.875rem;font-weight:500}';
    html += '.chat-panel{position:fixed;bottom:100px;right:24px;width:380px;max-width:calc(100vw - 48px);height:480px;max-height:calc(100vh - 150px);background:white;border-radius:18px;box-shadow:0 8px 40px rgba(0,0,0,0.16);display:none;flex-direction:column;z-index:998;overflow:hidden}.chat-panel.active{display:flex}';
    html += '.chat-header{background:#1e3a5f;color:white;padding:1rem 1.25rem;display:flex;justify-content:space-between;align-items:center}.chat-header h3{margin:0;font-size:0.9375rem;font-weight:600}.chat-close{background:none;border:none;color:white;font-size:1.25rem;cursor:pointer;opacity:0.7}.chat-close:hover{opacity:1}';
    html += '.chat-messages{flex:1;overflow-y:auto;padding:1rem;display:flex;flex-direction:column;gap:0.75rem;background:#f5f5f7}';
    html += '.chat-message{max-width:85%;padding:0.75rem 1rem;border-radius:16px;font-size:0.875rem;line-height:1.4}.chat-message.user{background:#1e3a5f;color:white;align-self:flex-end;border-bottom-right-radius:4px}.chat-message.assistant{background:white;color:#1e3a5f;align-self:flex-start;border-bottom-left-radius:4px}';
    html += '.chat-typing{display:flex;gap:4px;padding:0.5rem;align-self:flex-start}.chat-typing span{width:8px;height:8px;background:#86868b;border-radius:50%;animation:typing 1.4s infinite}.chat-typing span:nth-child(2){animation-delay:0.2s}.chat-typing span:nth-child(3){animation-delay:0.4s}@keyframes typing{0%,60%,100%{transform:translateY(0)}30%{transform:translateY(-4px)}}';
    html += '.chat-input-area{padding:1rem;border-top:1px solid #f0f0f0;display:flex;gap:0.5rem}.chat-input{flex:1;padding:0.75rem 1rem;border:none;border-radius:16px;font-size:0.875rem;background:#f5f5f7;resize:none}.chat-input:focus{outline:none;background:#ebebed}.chat-send{background:#1e3a5f;color:white;border:none;border-radius:16px;padding:0.75rem 1rem;cursor:pointer;font-weight:500}.chat-send:hover{background:#0088c2}';

    // Loading spinner
    html += '.loading{display:flex;justify-content:center;align-items:center;padding:4rem}.spinner{width:40px;height:40px;border:3px solid #f0f0f0;border-top-color:#0088c2;border-radius:50%;animation:spin 1s linear infinite}@keyframes spin{to{transform:rotate(360deg)}}';

    // Responsive
    html += '@media(max-width:768px){.header{padding:0.75rem 1rem;height:auto}.stats-bar{padding:1rem;gap:1.5rem}.stat-value{font-size:1.25rem}.filters-bar{padding:0.75rem 1rem}.filter-select{padding:0.4rem 1.5rem 0.4rem 0.75rem;font-size:0.8125rem}.main{padding:1rem}.product-grid{gap:0.75rem;grid-template-columns:repeat(2,1fr)}.style-card{border-radius:12px}.style-image{height:140px}.style-info{padding:0.75rem}.style-name{font-size:0.8125rem}.chat-panel{width:calc(100vw - 32px);right:16px;bottom:90px}.modal-body{flex-direction:column}}';

    // Merchandising view styles
    html += '.merch-container{display:flex;flex-direction:column;gap:1.5rem;padding:1rem}';
    html += '.merch-section{background:#fff;border-radius:16px;padding:1.5rem;box-shadow:0 2px 8px rgba(0,0,0,0.06)}';
    html += '.merch-section h3{margin:0 0 1rem;font-size:1.1rem;color:#1e3a5f;display:flex;align-items:center;gap:0.5rem}';
    html += '.merch-row{display:flex;gap:2rem;flex-wrap:wrap}';
    html += '.merch-chart-box{flex:1;min-width:300px}';
    html += '.merch-donut-wrapper{position:relative;width:280px;height:280px;margin:0 auto}';
    html += '.merch-donut-center{position:absolute;top:50%;left:50%;transform:translate(-50%,-50%);text-align:center}';
    html += '.merch-donut-center .value{font-size:1.5rem;font-weight:700;color:#1e3a5f}';
    html += '.merch-donut-center .label{font-size:0.75rem;color:#86868b}';
    html += '.merch-legend{flex:1;min-width:280px;max-height:320px;overflow-y:auto}';
    html += '.merch-legend-item{display:flex;align-items:center;gap:0.75rem;padding:0.6rem 0.5rem;border-radius:8px;cursor:pointer;transition:background 0.15s}';
    html += '.merch-legend-item:hover{background:#f5f5f7}';
    html += '.merch-legend-color{width:14px;height:14px;border-radius:4px;flex-shrink:0}';
    html += '.merch-legend-info{flex:1;min-width:0}';
    html += '.merch-legend-name{font-size:0.875rem;font-weight:500;color:#1e3a5f;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}';
    html += '.merch-legend-stats{font-size:0.75rem;color:#86868b}';
    html += '.merch-legend-badge{background:#fff3cd;color:#856404;font-size:0.65rem;padding:2px 6px;border-radius:4px;font-weight:600;margin-left:auto;flex-shrink:0}';
    html += '.merch-bubble-area{width:100%;height:380px;position:relative;background:#fafafa;border-radius:8px}';
    html += '.merch-controls{display:flex;gap:1rem;margin-bottom:1rem;flex-wrap:wrap;align-items:center}';
    html += '.merch-radio-group{display:flex;gap:0.75rem;align-items:center}';
    html += '.merch-radio-group label{display:flex;align-items:center;gap:0.35rem;font-size:0.8125rem;color:#4a5568;cursor:pointer}';
    html += '.merch-radio-group input{accent-color:#0088c2}';
    html += '.scorecard-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1rem;margin-bottom:1.25rem}';
    html += '.scorecard-metric{background:#f5f5f7;border-radius:12px;padding:1rem;text-align:center}';
    html += '.scorecard-metric .value{font-size:1.75rem;font-weight:700;color:#1e3a5f}';
    html += '.scorecard-metric .label{font-size:0.75rem;color:#86868b;margin-top:0.25rem}';
    html += '.scorecard-health{padding:0.6rem 1.25rem;border-radius:8px;font-weight:600;display:inline-block}';
    html += '.scorecard-health.strong{background:#d4edda;color:#155724}';
    html += '.scorecard-health.moderate{background:#fff3cd;color:#856404}';
    html += '.scorecard-health.opportunity{background:#f8d7da;color:#721c24}';
    html += '.scorecard-top-list{margin-top:1rem}';
    html += '.scorecard-top-item{display:flex;justify-content:space-between;padding:0.5rem 0;border-bottom:1px solid #eee;font-size:0.875rem}';
    html += '.scorecard-top-item:last-child{border-bottom:none}';
    html += '.customer-select-row{display:flex;gap:1rem;align-items:center;margin-bottom:1.25rem}';
    html += '.customer-select-row label{font-weight:500;color:#1e3a5f}';
    html += '.customer-select-row select{padding:0.5rem 1rem;border:1px solid #ddd;border-radius:8px;font-size:0.875rem;min-width:280px}';
    // Color ranking styles - clean 5-column grid
    html += '.color-ranking-grid{display:grid;grid-template-columns:repeat(5,1fr);gap:0.5rem}';
    html += '.color-rank-item{display:flex;align-items:center;gap:0.5rem;padding:0.625rem;background:#f8fafc;border-radius:8px;transition:all 0.2s}';
    html += '.color-rank-item:hover{background:#e8f4fc;transform:translateY(-1px)}';
    html += '.color-rank-num{font-size:0.75rem;font-weight:600;color:#86868b;min-width:1.25rem}';
    html += '.color-rank-swatch{width:20px;height:20px;border-radius:4px;flex-shrink:0;border:1px solid rgba(0,0,0,0.1)}';
    html += '.color-rank-name{font-size:0.8125rem;font-weight:600;color:#1e3a5f;flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}';
    html += '.color-rank-bar-wrap{flex:1;min-width:60px;height:8px;background:#e5e5e5;border-radius:4px;overflow:hidden}';
    html += '.color-rank-bar{height:100%;border-radius:4px;transition:width 0.3s}';
    html += '.color-rank-value{font-size:0.8125rem;font-weight:600;color:#0088c2;min-width:50px;text-align:right}';
    html += '.color-rank-meta{font-size:0.6875rem;color:#86868b;white-space:nowrap}';
    html += '@media(max-width:1200px){.color-ranking-grid{grid-template-columns:repeat(4,1fr)}}';
    html += '@media(max-width:900px){.color-ranking-grid{grid-template-columns:repeat(3,1fr)}}';
    html += '@media(max-width:600px){.color-ranking-grid{grid-template-columns:repeat(2,1fr)}}';

    html += '</style></head><body>';

    // Header
    html += '<header class="header">';
    html += '<div class="mode-toggle"><button class="mode-btn active" data-mode="sales" onclick="switchMode(\'sales\')">📦 Sales Orders</button><button class="mode-btn" data-mode="po" onclick="switchMode(\'po\')">🚢 Import POs</button></div>';
    html += '<h1><span style="color:#1e3a5f">Mark Edwards</span> <span id="dashboardTitle">Open Orders</span> Dashboard</h1>';
    html += '<div class="header-right">';
    html += '<button class="btn btn-secondary" onclick="showSettingsModal()">⚙️ Settings</button>';
    html += '<button class="btn btn-secondary" onclick="showUploadModal()">Import CSV</button>';
    html += '<a href="' + (process.env.PRODUCT_CATALOG_URL || 'https://catalogue.markedwards.cloud') + '" class="btn btn-secondary" target="_blank">Product Catalog (ATS)</a>';
    html += '</div></header>';

    // Stats bar
    html += '<div class="stats-bar">';
    html += '<div class="stat-box"><div class="stat-value" id="statOrders">-</div><div class="stat-label">Open Orders</div></div>';
    html += '<div class="stat-box"><div class="stat-value" id="statCustomers">-</div><div class="stat-label">Customers</div></div>';
    html += '<div class="stat-box"><div class="stat-value" id="statStyles">-</div><div class="stat-label">Styles</div></div>';
    html += '<div class="stat-box"><div class="stat-value" id="statUnits">-</div><div class="stat-label">Units</div></div>';
    html += '<div class="stat-box highlight" id="statDollarsBox" style="display:none;position:relative"><div class="stat-value money" id="statDollars">-</div><div class="stat-label">Total Value</div><button onclick="relockTotalValue()" title="Hide total value" style="position:absolute;top:6px;right:8px;background:none;border:none;cursor:pointer;font-size:0.75rem;opacity:0.4;padding:2px" onmouseover="this.style.opacity=1" onmouseout="this.style.opacity=0.4">🔒</button></div>';
    html += '<div class="stat-box" id="statDollarsLock" style="cursor:pointer;opacity:0.5" onclick="showPinModal()" title="Unlock to view total value"><div class="stat-value" style="font-size:1.5rem">🔒</div><div class="stat-label">Total Value</div></div>';
    html += '</div>';

    // Filters bar
    html += '<div class="filters-bar">';
    html += '<div class="filter-group"><label class="filter-label">Cal Year</label><div class="multi-select" id="yearMultiSelect"><div class="multi-select-display" onclick="toggleMultiSelect(\'year\')"><span id="yearDisplay">All Years</span><span class="multi-select-arrow">▼</span></div><div class="multi-select-dropdown" id="yearDropdown"></div></div></div>';
    html += '<div class="filter-group"><label class="filter-label">Fiscal Year</label><div class="multi-select" id="fyMultiSelect"><div class="multi-select-display" onclick="toggleMultiSelect(\'fy\')"><span id="fyDisplay">All FY</span><span class="multi-select-arrow">▼</span></div><div class="multi-select-dropdown" id="fyDropdown"></div></div></div>';
    html += '<div class="filter-group"><label class="filter-label">Customer</label><div class="multi-select" id="customerMultiSelect"><div class="multi-select-display" onclick="toggleMultiSelect(\'customer\')"><span id="customerDisplay">All Customers</span><span class="multi-select-arrow">▼</span></div><div class="multi-select-dropdown" id="customerDropdown"></div></div></div>';
    html += '<div class="filter-group"><label class="filter-label">Delivery</label><div class="multi-select" id="monthMultiSelect"><div class="multi-select-display" onclick="toggleMultiSelect(\'month\')"><span id="monthDisplay">All Months</span><span class="multi-select-arrow">▼</span></div><div class="multi-select-dropdown" id="monthDropdown"></div></div></div>';
    html += '<div class="filter-group"><label class="filter-label">Commodity</label><div class="multi-select" id="commodityMultiSelect"><div class="multi-select-display" onclick="toggleMultiSelect(\'commodity\')"><span id="commodityDisplay">All Commodities</span><span class="multi-select-arrow">▼</span></div><div class="multi-select-dropdown" id="commodityDropdown"></div></div></div>';
    html += '<div class="filter-group"><label class="filter-label">Department</label><div class="multi-select" id="departmentMultiSelect"><div class="multi-select-display" onclick="toggleMultiSelect(\'department\')"><span id="departmentDisplay">All Departments</span><span class="multi-select-arrow">▼</span></div><div class="multi-select-dropdown" id="departmentDropdown"></div></div></div>';
    html += '<div class="filter-group"><label class="filter-label">Style Search</label><div style="position:relative"><input type="text" id="styleSearchInput" placeholder="Search styles..." style="padding:0.5rem 0.75rem;border:1px solid #d1d5db;border-radius:6px;font-size:0.8125rem;width:140px" onkeyup="handleStyleSearch(event)"><button onclick="clearStyleSearch()" id="styleSearchClear" style="position:absolute;right:6px;top:50%;transform:translateY(-50%);background:none;border:none;cursor:pointer;color:#86868b;display:none;font-size:1rem">×</button></div></div>';
    html += '<div class="status-toggle"><button class="status-btn active" data-status="Open">Open</button><button class="status-btn" data-status="Invoiced">Invoiced</button><button class="status-btn" data-status="All">All</button></div>';
    html += '<button class="clear-filters" onclick="clearFilters()" style="display:none" id="clearFiltersBtn">Clear Filters</button>';
    html += '<div class="filter-group"><label class="filter-label">Sort By</label><select class="filter-select" id="sortByFilter"><option value="value">$ Value (High→Low)</option><option value="units">Units (High→Low)</option><option value="orders">Most Orders</option><option value="commodity">Commodity</option></select></div>';
    html += '<div class="view-toggle"><button class="view-btn active" data-view="dashboard">📊 Dashboard</button><button class="view-btn" data-view="summary">Summary</button><button class="view-btn" data-view="styles">By Style</button><button class="view-btn" data-view="topmovers">🏆 Top Movers</button><button class="view-btn" data-view="opportunities">🎯 Opportunities</button><button class="view-btn" data-view="orders">By SO#</button><button class="view-btn" data-view="charts">Charts</button><button class="view-btn" data-view="merchandising">📈 Merchandising</button></div>';
    html += '</div>';

    // Main content
    html += '<main class="main"><div id="content"><div class="loading"><div class="spinner"></div></div></div></main>';

    // Style detail modal
    html += '<div class="modal" id="styleModal"><div class="modal-content"><button class="modal-close" onclick="closeModal()">&times;</button><div class="modal-body"><div class="modal-image"><img id="modalImage" src="" alt=""></div><div class="modal-details"><div class="modal-header"><h2 id="modalStyleName">-</h2><p id="modalStyleNumber">-</p></div><div class="modal-stats"><div class="modal-stat"><div class="modal-stat-value" id="modalQty">-</div><div class="modal-stat-label">Total Units</div></div><div class="modal-stat"><div class="modal-stat-value money" id="modalDollars">-</div><div class="modal-stat-label">Total Value</div></div><div class="modal-stat"><div class="modal-stat-value" id="modalOrders">-</div><div class="modal-stat-label">Orders</div></div></div><div class="orders-list"><h4>Orders</h4><div id="modalOrdersList"></div></div></div></div></div></div>';

    // Upload modal
    html += '<div class="modal upload-modal" id="uploadModal"><div class="modal-content"><button class="modal-close" onclick="closeUploadModal()">&times;</button><h2>Import Sales Orders</h2><p style="color:#86868b;margin-top:0.5rem">Upload a CSV file with your sales order data</p><div class="upload-area" id="uploadArea"><input type="file" id="fileInput" accept=".csv"><div class="upload-icon">📄</div><div class="upload-text">Drag & drop your CSV here or <strong>browse</strong></div></div><div class="upload-progress" id="uploadProgress"><div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div><p style="margin-top:0.5rem;font-size:0.875rem;color:#86868b" id="uploadStatus">Uploading...</p></div></div></div>';

    // Settings modal
    html += '<div class="modal settings-modal" id="settingsModal"><div class="modal-content" style="max-width:480px;padding:2rem;max-height:85vh;overflow-y:auto">';
    html += '<button class="modal-close" onclick="closeSettingsModal()">&times;</button>';
    html += '<h2>⚙️ Settings</h2>';

    // === SYNC DATA SECTION ===
    html += '<div style="margin-top:1.5rem;padding:1.25rem;background:linear-gradient(135deg,#f0fdf4,#ecfdf5);border:1px solid #bbf7d0;border-radius:0.75rem">';
    html += '<h3 style="margin:0 0 0.5rem 0;color:#166534;font-size:1rem">📤 Sync Data</h3>';
    html += '<p style="color:#4b5563;font-size:0.8125rem;margin-bottom:1rem">Export fresh data from Zoho Analytics and import it automatically.</p>';
    html += '<button class="btn btn-primary" onclick="triggerExportAndSync()" id="triggerExportBtn" style="width:100%;padding:0.875rem;font-size:0.9375rem;background:#16a34a;border:none;border-radius:0.5rem;color:#fff;cursor:pointer;font-weight:600">📤 Trigger Export & Sync</button>';
    html += '<div id="triggerExportResult" style="margin-top:0.75rem;font-size:0.8125rem;color:#4b5563;text-align:center"></div>';
    html += '</div>';

    // === STATUS SECTION ===
    html += '<div style="margin-top:1.25rem;padding:1rem;background:#f8fafc;border:1px solid #e2e8f0;border-radius:0.75rem">';
    html += '<h3 style="margin:0 0 0.75rem 0;color:#1e3a5f;font-size:0.9375rem">📊 Status</h3>';
    html += '<div style="display:grid;gap:0.5rem;font-size:0.8125rem">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center"><span style="color:#64748b">Token</span><span id="tokenStatusBadge">Checking...</span></div>';
    html += '<div style="display:flex;justify-content:space-between;align-items:center"><span style="color:#64748b">WorkDrive</span><span id="workdriveStatusBadge">Checking...</span></div>';
    html += '<div style="display:flex;justify-content:space-between;align-items:center"><span style="color:#64748b">Last Sync</span><span id="lastSyncBadge" style="color:#1e293b">-</span></div>';
    html += '<div style="display:flex;justify-content:space-between;align-items:center"><span style="color:#64748b">Images Cached</span><span id="imagesCachedBadge" style="color:#1e293b">-</span></div>';
    html += '</div>';
    html += '</div>';

    // === FISCAL YEAR SETTINGS ===
    html += '<div style="margin-top:1.25rem">';
    html += '<label style="font-weight:600;color:#1e3a5f;display:block;margin-bottom:0.5rem">Default Fiscal Year</label>';
    html += '<p style="color:#86868b;font-size:0.8125rem;margin-bottom:0.75rem">This fiscal year will be selected automatically when the dashboard loads.</p>';
    html += '<select id="defaultFYSelect" class="filter-select" style="width:100%;padding:0.75rem">';
    html += '<option value="">No Default (show all)</option>';
    html += '</select>';
    html += '</div>';
    html += '<div style="margin-top:1.25rem">';
    html += '<label style="font-weight:600;color:#1e3a5f;display:block;margin-bottom:0.5rem">Include Fiscal Years</label>';
    html += '<p style="color:#86868b;font-size:0.8125rem;margin-bottom:0.75rem">Only load data from selected fiscal years. Excluding old years speeds up the app.</p>';
    html += '<div id="fyCheckboxes" style="display:flex;flex-wrap:wrap;gap:0.5rem"></div>';
    html += '</div>';
    html += '<div style="margin-top:1.5rem;display:flex;gap:1rem">';
    html += '<button class="btn btn-primary" onclick="saveSettings()" style="flex:1">Save Settings</button>';
    html += '<button class="btn btn-secondary" onclick="closeSettingsModal()" style="flex:1">Cancel</button>';
    html += '</div>';

    // === ADVANCED SECTION (collapsible) ===
    html += '<div style="margin-top:1.5rem;border-top:1px solid #e5e5e5;padding-top:1rem">';
    html += '<button onclick="toggleAdvancedSettings()" id="advancedToggleBtn" style="background:none;border:none;cursor:pointer;font-size:0.875rem;color:#64748b;display:flex;align-items:center;gap:0.5rem;padding:0;font-weight:500"><span id="advancedArrow">▶</span> Advanced</button>';
    html += '<div id="advancedSettings" style="display:none;margin-top:1rem">';

    // Manual sync buttons
    html += '<div style="margin-bottom:1rem">';
    html += '<label style="font-weight:600;color:#1e3a5f;display:block;margin-bottom:0.5rem;font-size:0.875rem">Manual Sync</label>';
    html += '<div style="display:flex;gap:0.5rem;flex-wrap:wrap">';
    html += '<button class="btn btn-secondary" onclick="syncFromWorkDrive()" id="workdriveSyncBtn" style="flex:1;font-size:0.8125rem;padding:0.5rem">🔄 Sync Sales Orders</button>';
    html += '<button class="btn btn-secondary" onclick="syncImportPOs()" id="importPOSyncBtn" style="flex:1;font-size:0.8125rem;padding:0.5rem">📦 Sync Import POs</button>';
    html += '</div>';
    html += '<div id="workdriveSyncResult" style="margin-top:0.5rem;font-size:0.75rem;color:#86868b"></div>';
    html += '<div id="importPOSyncResult" style="margin-top:0.5rem;font-size:0.75rem;color:#86868b"></div>';
    html += '</div>';

    // Image pre-caching
    html += '<div style="margin-bottom:1rem">';
    html += '<label style="font-weight:600;color:#1e3a5f;display:block;margin-bottom:0.5rem;font-size:0.875rem">Image Pre-Caching</label>';
    html += '<div style="display:flex;gap:0.5rem;align-items:center">';
    html += '<button class="btn btn-secondary" onclick="startPreCache()" id="preCacheBtn" style="flex:1;font-size:0.8125rem;padding:0.5rem">🖼️ Pre-Cache Images</button>';
    html += '<span id="preCacheStatus" style="font-size:0.75rem;color:#86868b"></span>';
    html += '</div>';
    html += '</div>';

    // Token refresh
    html += '<div style="margin-bottom:1rem">';
    html += '<label style="font-weight:600;color:#1e3a5f;display:block;margin-bottom:0.5rem;font-size:0.875rem">Zoho Token</label>';
    html += '<div id="tokenStatus" style="display:none"></div>';
    html += '<div style="display:flex;gap:0.5rem;align-items:center">';
    html += '<button class="btn btn-secondary" onclick="refreshZohoToken()" id="refreshTokenBtn" style="flex:1;font-size:0.8125rem;padding:0.5rem">🔑 Refresh Token</button>';
    html += '<span id="refreshTokenStatus" style="font-size:0.75rem;color:#86868b"></span>';
    html += '</div>';
    html += '</div>';

    // WorkDrive folder browser
    html += '<div>';
    html += '<label style="font-weight:600;color:#1e3a5f;display:block;margin-bottom:0.5rem;font-size:0.875rem">WorkDrive Folder</label>';
    html += '<button class="btn btn-secondary" onclick="toggleFolderBrowser()" id="workdriveBrowseBtn" style="width:100%;font-size:0.8125rem;padding:0.5rem">📂 Browse Folders</button>';
    html += '<div id="workdriveFolderBrowser" style="display:none;margin-top:0.75rem;padding:0.75rem;background:#fff;border:1px solid #e5e5e5;border-radius:0.5rem">';
    html += '<div style="font-weight:600;font-size:0.8125rem;color:#1e3a5f;margin-bottom:0.5rem">📂 Enter Folder Details from URL</div>';
    html += '<div style="font-size:0.75rem;color:#86868b;margin-bottom:0.75rem">From your WorkDrive URL, copy the IDs:</div>';
    html += '<div style="display:flex;flex-direction:column;gap:0.5rem;margin-bottom:0.75rem">';
    html += '<div style="display:flex;gap:0.5rem;align-items:center">';
    html += '<label style="font-size:0.75rem;color:#1e3a5f;min-width:60px">Team ID:</label>';
    html += '<input type="text" id="manualTeamId" placeholder="f7owk7c2a52496d5749b194f523c32d8fa8c5" style="flex:1;padding:0.5rem;font-size:0.75rem;border:1px solid #e5e5e5;border-radius:4px">';
    html += '</div>';
    html += '<div style="display:flex;gap:0.5rem;align-items:center">';
    html += '<label style="font-size:0.75rem;color:#1e3a5f;min-width:60px">Folder ID:</label>';
    html += '<input type="text" id="manualFolderId" placeholder="tuxp28e05541e31b84b40a46dbaf132b421e1" style="flex:1;padding:0.5rem;font-size:0.75rem;border:1px solid #e5e5e5;border-radius:4px">';
    html += '</div>';
    html += '<button onclick="testFolderId()" style="padding:0.5rem 0.75rem;font-size:0.75rem;background:#007aff;color:#fff;border:none;border-radius:4px;cursor:pointer;margin-top:0.25rem">Test Connection</button>';
    html += '</div>';
    html += '<div style="font-size:0.7rem;color:#86868b;background:#f9f9f9;padding:0.5rem;border-radius:4px;margin-bottom:0.5rem">';
    html += '<strong>From URL:</strong> workdrive.zoho.com/<span style="color:#007aff">TEAM_ID</span>/teams/.../folders/<span style="color:#34c759">FOLDER_ID</span>';
    html += '</div>';
    html += '<div id="folderBreadcrumb" style="font-size:0.75rem;color:#86868b;margin-bottom:0.5rem"></div>';
    html += '<div id="folderList" style="max-height:100px;overflow-y:auto"></div>';
    html += '</div>';
    html += '</div>';

    html += '</div>'; // end advancedSettings

    // Management PIN section (inside advanced wrapper but after collapsible)
    html += '<div style="margin-top:1rem;border-top:1px solid #e5e5e5;padding-top:1rem">';
    html += '<label style="font-weight:600;color:#1e3a5f;display:block;margin-bottom:0.5rem;font-size:0.875rem">🔒 Management PIN</label>';
    html += '<p style="color:#86868b;font-size:0.75rem;margin-bottom:0.5rem">Set a PIN to control who can see Total Value. Leave blank to show to everyone.</p>';
    html += '<div style="display:flex;gap:0.5rem">';
    html += '<input type="password" id="mgmtPinInput" placeholder="Set PIN (e.g. 1234)" maxlength="10" style="flex:1;padding:0.5rem;font-size:0.8125rem;border:1px solid #e5e5e5;border-radius:4px">';
    html += '<button class="btn btn-secondary" onclick="saveMgmtPin()" style="font-size:0.8125rem;padding:0.5rem 1rem">Save PIN</button>';
    html += '</div>';
    html += '<div id="mgmtPinStatus" style="font-size:0.75rem;margin-top:0.25rem;min-height:1rem"></div>';
    html += '<button onclick="clearMgmtPin()" style="background:none;border:none;cursor:pointer;font-size:0.75rem;color:#ff3b30;margin-top:0.25rem;padding:0">Remove PIN (show value to everyone)</button>';
    html += '</div>';
    html += '</div>'; // end advanced wrapper
    html += '<div id="settingsStatus" style="margin-top:1rem;text-align:center;font-size:0.875rem"></div>';
    html += '</div></div>';

    // PIN unlock modal
    html += '<div class="modal" id="pinModal"><div class="modal-content" style="max-width:340px;padding:2rem;text-align:center">';
    html += '<button class="modal-close" onclick="closePinModal()">&times;</button>';
    html += '<h2 style="margin-bottom:0.5rem">🔒 Enter PIN</h2>';
    html += '<p style="color:#86868b;font-size:0.875rem;margin-bottom:1.25rem">Enter the management PIN to view total values.</p>';
    html += '<input type="password" id="pinInput" maxlength="10" placeholder="Enter PIN" style="width:100%;padding:0.75rem;font-size:1.25rem;text-align:center;border:1px solid #d1d5db;border-radius:8px;letter-spacing:0.25rem" onkeyup="if(event.key===\'Enter\')verifyPin()">';
    html += '<div id="pinError" style="color:#ff3b30;font-size:0.8125rem;margin-top:0.5rem;min-height:1.25rem"></div>';
    html += '<button class="btn btn-primary" onclick="verifyPin()" style="width:100%;margin-top:0.75rem;padding:0.75rem">Unlock</button>';
    html += '</div></div>';
    html += '<div class="chat-bubble" onclick="toggleChat()"><svg viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z"/></svg><span class="chat-bubble-label">Ask AI</span></div>';

    // Chat panel
    html += '<div class="chat-panel" id="chatPanel"><div class="chat-header"><h3>AI Assistant</h3><button class="chat-close" onclick="toggleChat()">&times;</button></div><div class="chat-messages" id="chatMessages"><div class="chat-message assistant">Hi! I can help you find orders. Try asking things like:<br><br>• "Show me Burlington\'s orders for June"<br>• "What denim orders are shipping next month?"<br>• "Find Ross orders"</div></div><div class="chat-input-area"><textarea class="chat-input" id="chatInput" placeholder="Ask about orders..." rows="1"></textarea><button class="chat-send" onclick="sendChat()">Send</button></div></div>';

    // JavaScript
    html += '<script>';

    // State
    html += 'var state = { mode: "sales", filters: { years: [], fiscalYears: [], customers: [], vendors: [], months: [], commodities: [], departments: [], status: "Open", poStatus: "Open" }, view: "dashboard", sortBy: "value", data: null, summaryGroupBy: "commodity", expandedRows: {}, groupByCustomer: false, groupByVendor: false, stackByStyle: false, expandedStacks: {}, treemapView: "commodity", styleSearch: "", poTilesLimit: 200 };';

    // Store all months for filtering
    html += 'var allMonths = [];';

    // Load filters
    html += 'async function loadFilters() {';
    html += 'try { var res = await fetch("/api/filters"); var data = await res.json();';
    html += 'var currentYear = new Date().getFullYear();';
    // Cal Year multi-select
    html += 'data.years = data.years || [];';
    html += 'if (data.years.length === 0) { for(var y = currentYear; y >= currentYear - 3; y--) data.years.push(y); }';
    html += 'populateMultiSelect("year", data.years.map(function(y) { return { value: y, label: String(y) }; }), "All Years");';
    // Fiscal Year multi-select
    html += 'data.fiscalYears = data.fiscalYears || [];';
    html += 'if (data.fiscalYears.length === 0) { for(var fy = currentYear + 1; fy >= currentYear - 3; fy--) data.fiscalYears.push(fy); }';
    html += 'populateMultiSelect("fy", data.fiscalYears.map(function(fy) { return { value: fy, label: "FY" + fy }; }), "All FY");';
    // Customer multi-select
    html += 'populateMultiSelect("customer", data.customers.map(function(c) { return { value: c.customer, label: c.customer + " (" + c.order_count + ")" }; }), "All Customers");';
    // Store all months for contextual filtering
    html += 'allMonths = data.months || [];';
    // Month multi-select - initially show all
    html += 'populateMultiSelect("month", allMonths.map(function(m) { return { value: m.month, label: m.display_name }; }), "All Months");';
    // Commodity multi-select
    html += 'populateMultiSelect("commodity", data.commodities.map(function(c) { return { value: c.commodity, label: c.commodity + " (" + c.style_count + ")" }; }), "All Commodities");';
    html += 'var deptMap = { J:"J - Juniors", Y:"Y - Ext Juniors", P:"P - Big Kids", W:"W - Missy (Senior Women)", M:"M - Maternity", S:"S - Missy (Petite)", K:"K - Ext Kids", L:"L - Little Kids", Z:"Z - Missy (Plus Size)", C:"C - Boys & Unisex Kids", B:"B - Baby 0-3", I:"I - Infants (12M-24M)", T:"T - Toddlers", U:"U - Men" };';
    html += 'var deptOptions = (data.departments || []).filter(function(d) { return d.dept_code && deptMap[d.dept_code]; }).map(function(d) { return { value: d.dept_code, label: deptMap[d.dept_code] + " (" + d.style_count + ")" }; });';
    html += 'populateMultiSelect("department", deptOptions, "All Departments");';
    html += '} catch(e) { console.error("Error loading filters:", e); }}';

    // Filter months based on selected FY/Year
    html += 'function filterMonthsDropdown() {';
    html += 'var filteredMonths = allMonths;';
    // Filter by fiscal year if selected
    html += 'if (state.filters.fiscalYears.length > 0) {';
    html += 'filteredMonths = allMonths.filter(function(m) {';
    html += 'var parts = m.month.split("-");';
    html += 'var year = parseInt(parts[0]);';
    html += 'var month = parseInt(parts[1]);';
    // FY runs June (prev year) to May (FY year) - e.g. FY2026 = June 2025 - May 2026
    html += 'return state.filters.fiscalYears.some(function(fy) {';
    html += 'var fyStart = new Date(fy - 1, 5, 1);'; // June 1 of previous year
    html += 'var fyEnd = new Date(fy, 4, 31);'; // May 31 of FY year
    html += 'var monthDate = new Date(year, month - 1, 15);';
    html += 'return monthDate >= fyStart && monthDate <= fyEnd;';
    html += '});';
    html += '});';
    html += '}';
    // Filter by calendar year if selected (and no FY selected)
    html += 'else if (state.filters.years.length > 0) {';
    html += 'filteredMonths = allMonths.filter(function(m) {';
    html += 'var year = parseInt(m.month.split("-")[0]);';
    html += 'return state.filters.years.indexOf(year) !== -1;';
    html += '});';
    html += '}';
    // Re-populate the dropdown
    html += 'populateMultiSelect("month", filteredMonths.map(function(m) { return { value: m.month, label: m.display_name }; }), "All Months");';
    // Clear selected months that are no longer in the filtered list
    html += 'var validMonths = filteredMonths.map(function(m) { return m.month; });';
    html += 'state.filters.months = state.filters.months.filter(function(m) { return validMonths.indexOf(m) !== -1; });';
    html += 'if (state.filters.months.length === 0) { document.getElementById("monthDisplay").textContent = "All Months"; document.querySelector("#monthMultiSelect .multi-select-display").classList.remove("active"); }';
    html += '}';

    // Load data
    html += 'async function loadData() {';
    html += 'document.getElementById("content").innerHTML = \'<div class="loading"><div class="spinner"></div></div>\';';
    html += 'try {';
    html += 'var params = new URLSearchParams();';
    html += 'if (state.styleSearch) params.append("styleSearch", state.styleSearch);';
    html += 'if (state.mode === "po") {';
    // PO mode - pass vendor filter (stored in state.filters.customers when in PO mode)
    html += 'if (state.filters.customers.length > 0) params.append("vendors", state.filters.customers.join("||"));';
    html += 'if (state.filters.commodities.length > 0) params.append("commodities", state.filters.commodities.join(","));';
    html += 'if (state.filters.departments.length > 0) params.append("departments", state.filters.departments.join(","));';
    html += 'if (state.filters.months.length > 0) params.append("months", state.filters.months.join(","));';
    html += 'params.append("status", state.filters.poStatus || "Open");';
    html += 'var res = await fetch("/api/po/orders?" + params.toString());';
    html += 'var data = await res.json();';
    html += 'state.data = data;';
    html += 'updateStatsPO(data.stats || {});';
    html += 'renderPOContent(data);';
    html += '} else {';
    // Sales mode - all filters now use arrays
    html += 'if (state.filters.years.length > 0) params.append("years", state.filters.years.join(","));';
    html += 'if (state.filters.fiscalYears.length > 0) params.append("fiscalYears", state.filters.fiscalYears.join(","));';
    html += 'if (state.filters.customers.length > 0) params.append("customers", state.filters.customers.join("||"));';
    html += 'if (state.filters.months.length > 0) params.append("months", state.filters.months.join(","));';
    html += 'if (state.filters.commodities.length > 0) params.append("commodities", state.filters.commodities.join(","));';
    html += 'if (state.filters.departments.length > 0) params.append("departments", state.filters.departments.join(","));';
    html += 'if (state.filters.status) params.append("status", state.filters.status);';
    html += 'var url = state.view === "orders" ? "/api/orders/by-so?" : "/api/orders?";';
    html += 'var controller = new AbortController();';
    html += 'var timeoutId = setTimeout(function() { controller.abort(); }, 60000);'; // 60 second timeout
    html += 'var res = await fetch(url + params.toString(), { signal: controller.signal });';
    html += 'clearTimeout(timeoutId);';
    html += 'var data = await res.json();';
    html += 'state.data = data;';
    html += 'updateStats(data.stats || {});';
    html += 'renderContent(data);';
    html += '}';
    html += '} catch(e) { console.error("Error loading data:", e); document.getElementById("content").innerHTML = \'<div class="empty-state"><h3>Error loading data</h3><p>\' + e.message + \'</p></div>\'; }}';

    // Update stats
    html += 'function updateStats(stats) {';
    html += 'document.getElementById("statOrders").textContent = formatNumber(stats.order_count || 0);';
    html += 'document.getElementById("statCustomers").textContent = formatNumber(stats.customer_count || 0);';
    html += 'document.getElementById("statStyles").textContent = formatNumber(stats.style_count || 0);';
    html += 'document.getElementById("statUnits").textContent = formatNumber(stats.total_qty || 0);';
    html += 'document.getElementById("statDollars").textContent = formatMoney(stats.total_dollars || 0);}';

    // Update stats for PO mode
    html += 'function updateStatsPO(stats) {';
    html += 'document.getElementById("statOrders").textContent = formatNumber(stats.po_count || 0);';
    html += 'document.getElementById("statCustomers").textContent = formatNumber(stats.vendor_count || 0);';
    html += 'document.getElementById("statStyles").textContent = formatNumber(stats.style_count || 0);';
    html += 'document.getElementById("statUnits").textContent = formatNumber(stats.total_qty || 0);';
    html += 'document.getElementById("statDollars").textContent = formatMoney(stats.total_dollars || 0);}';

    // Render PO content - routes to different views based on state.view
    html += 'function renderPOContent(data) {';
    html += 'var container = document.getElementById("content");';
    html += 'try {';
    html += 'if (state.view === "dashboard") { renderPODashboard(container, data); }';
    html += 'else if (state.view === "styles") { renderPOStyles(container, data); }';
    html += 'else if (state.view === "orders") { renderPOByVendor(container, data); }';
    html += 'else if (state.view === "summary") { renderPOSummary(container, data); }';
    html += 'else if (state.view === "pricecompare") { renderPOPriceCompare(container, data); }';
    html += 'else if (state.view === "charts") { renderPOCharts(container, data); }';
    html += 'else { renderPODashboard(container, data); }';
    html += '} catch(e) { console.error("PO Render error:", e); container.innerHTML = \'<div class="empty-state"><h3>Render Error</h3><p>\' + e.message + \'</p></div>\'; }}';

    // PO Dashboard view
    html += 'function renderPODashboard(container, data) {';
    html += 'var items = data.items || [];';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No Import POs found</h3><p>Import data with PO information to see results</p></div>\'; return; }';
    html += 'var colors = ["#1e3a5f", "#0088c2", "#4da6d9", "#34c759", "#ff9500", "#ff3b30", "#af52de", "#5856d6", "#00c7be", "#86868b"];';
    html += 'var vendorData = data.vendorBreakdown || [];';
    html += 'var commData = data.commodityBreakdown || [];';
    html += 'var monthlyData = data.monthlyBreakdown || [];';
    html += 'var total = vendorData.reduce(function(a,v) { return a + parseFloat(v.total_dollars||0); }, 0);';
    html += 'var out = "";';
    // Month timeline
    html += 'out += \'<div class="dashboard-timeline"><span class="timeline-title">📅 ETA Months:</span><div class="timeline-bars">\';';
    html += 'var months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];';
    html += 'var prevYear = null;';
    html += 'monthlyData.forEach(function(m, idx) {';
    html += 'if (!m.month) return;';
    html += 'var parts = m.month.split("-");';
    html += 'var year = parts[0];';
    html += 'var monthName = months[parseInt(parts[1])-1];';
    html += 'var nextM = monthlyData[idx + 1];';
    html += 'var nextYear = nextM && nextM.month ? nextM.month.split("-")[0] : null;';
    html += 'if (prevYear !== year) { out += \'<div class="timeline-year-group"><div class="timeline-year-label">\\x27\' + year.slice(-2) + \'</div><div class="timeline-year-months">\'; }';
    html += 'prevYear = year;';
    html += 'var monthKey = m.month;';
    html += 'var isActive = state.filters.months.indexOf(monthKey) !== -1;';
    html += 'out += \'<div class="timeline-month\' + (isActive ? " active" : "") + \'" onclick="filterByMonthPO(\\x27\' + monthKey + \'\\x27)">\';';
    html += 'out += \'<div class="timeline-bar"><span class="bar-month">\' + monthName + \'</span></div>\';';
    html += 'out += \'<div class="timeline-stats"><span class="timeline-dollars">$\' + Math.round(parseFloat(m.total_dollars)/1000).toLocaleString() + \'K</span></div></div>\';';
    html += 'if (nextYear !== year) { out += \'</div></div>\'; }';
    html += '});';
    html += 'out += \'</div></div>\';';
    // Main layout
    html += 'out += \'<div class="dashboard-layout">\';';
    // Left sidebar - charts
    html += 'out += \'<div class="dashboard-charts">\';';
    // Monthly trends stacked bar chart - use monthlyByCommodity from API
    html += 'var monthlyByComm = {};';
    html += 'var allComms = {};';
    html += '(data.monthlyByCommodity || []).forEach(function(row) {';
    html += 'var mk = row.month || "Unknown";';
    html += 'var comm = row.commodity || "Other";';
    html += 'var value = parseFloat(row.total_dollars) || 0;';
    html += 'if (!monthlyByComm[mk]) monthlyByComm[mk] = {};';
    html += 'monthlyByComm[mk][comm] = (monthlyByComm[mk][comm] || 0) + value;';
    html += 'allComms[comm] = (allComms[comm] || 0) + value;';
    html += '});';
    html += 'var topComms = Object.entries(allComms).sort(function(a,b) { return b[1] - a[1]; }).slice(0,5).map(function(e) { return e[0]; });';
    html += 'var displayMonths = Object.keys(monthlyByComm).filter(function(m) { return m !== "Unknown" && m; }).sort();';
    html += 'var displayMax = displayMonths.length > 0 ? Math.max.apply(null, displayMonths.map(function(m) { var md = monthlyByComm[m] || {}; return Object.values(md).reduce(function(a,v) { return a+v; }, 0); })) : 1;';
    html += 'out += \'<div class="dashboard-card"><h3>📊 Monthly Trends <span style="float:right;font-size:0.75rem;color:#34c759;font-weight:600">$\' + (total/1000000).toFixed(1) + \'M total</span></h3>\';';
    html += 'out += \'<div class="mini-stacked-container">\';';
    html += 'out += \'<button class="mini-stacked-scroll-btn left" onclick="document.getElementById(\\x27poStackedBarWrapper\\x27).scrollBy({left:-150,behavior:\\x27smooth\\x27})">◀</button>\';';
    html += 'out += \'<div class="mini-stacked-wrapper" id="poStackedBarWrapper"><div class="mini-stacked">\';';
    html += 'var monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];';
    html += 'displayMonths.forEach(function(monthKey) {';
    html += 'var monthData = monthlyByComm[monthKey] || {};';
    html += 'var monthTotal = Object.values(monthData).reduce(function(a,v) { return a + v; }, 0);';
    html += 'var barHeight = displayMax > 0 ? (monthTotal / displayMax * 100) : 0;';
    html += 'var parts = monthKey.split("-");';
    html += 'var monthName = monthNames[parseInt(parts[1])-1] + " \\x27" + parts[0].slice(2);';
    html += 'out += \'<div class="mini-stacked-bar" style="height:100%">\';';
    html += 'out += \'<div style="height:\' + barHeight + \'%;display:flex;flex-direction:column;justify-content:flex-end">\';';
    html += 'topComms.forEach(function(comm, idx) {';
    html += 'var value = monthData[comm] || 0;';
    html += 'var segPct = monthTotal > 0 ? (value / monthTotal * 100) : 0;';
    html += 'if (segPct > 0) { out += \'<div class="mini-stacked-segment" style="height:\' + segPct + \'%;background:\' + colors[idx] + \'" title="\' + comm + \': $\' + Math.round(value/1000) + \'K"></div>\'; }';
    html += '});';
    html += 'out += \'</div><div class="mini-stacked-label">\' + monthName + \'</div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += 'out += \'<button class="mini-stacked-scroll-btn right" onclick="document.getElementById(\\x27poStackedBarWrapper\\x27).scrollBy({left:150,behavior:\\x27smooth\\x27})">▶</button>\';';
    html += 'out += \'</div>\';';
    // Legend
    html += 'out += \'<div class="mini-stacked-legend">\';';
    html += 'topComms.forEach(function(comm, idx) {';
    html += 'out += \'<div style="display:flex;align-items:center;gap:4px"><div style="width:10px;height:10px;border-radius:2px;background:\' + colors[idx] + \'"></div><span style="font-size:0.65rem;color:#666">\' + comm + \'</span></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    // Vendor treemap with onclick
    html += 'out += \'<div class="dashboard-card"><h3>🏭 By Vendor <span style="font-size:0.75rem;color:#86868b">(click to filter)</span></h3><div class="dashboard-treemap">\';';
    html += 'vendorData.slice(0,10).forEach(function(v, idx) {';
    html += 'var value = parseFloat(v.total_dollars) || 0;';
    html += 'var pct = total > 0 ? (value / total * 100) : 0;';
    html += 'var size = Math.max(Math.sqrt(pct) * 10, 12);';
    html += 'var vendorName = v.vendor_name || "Unknown";';
    html += 'out += \'<div class="treemap-item" style="flex-basis:\' + size + \'%;background:\' + colors[idx % colors.length] + \'" onclick="filterByVendor(\\x27\' + vendorName.replace(/\'/g, "\\\\\'") + \'\\x27)">\';';
    html += 'out += \'<div class="treemap-label">\' + vendorName + \'</div>\';';
    html += 'out += \'<div class="treemap-value">$\' + Math.round(value/1000).toLocaleString() + \'K</div>\';';
    html += 'out += \'<div class="treemap-pct">\' + pct.toFixed(1) + \'%</div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    // Commodity breakdown with onclick
    html += 'out += \'<div class="dashboard-card"><h3>📦 By Commodity <span style="font-size:0.75rem;color:#86868b">(click to filter)</span></h3><div class="dashboard-treemap">\';';
    html += 'commData.slice(0,8).forEach(function(c, idx) {';
    html += 'var value = parseFloat(c.total_dollars) || 0;';
    html += 'var pct = total > 0 ? (value / total * 100) : 0;';
    html += 'var size = Math.max(Math.sqrt(pct) * 10, 12);';
    html += 'var comm = c.commodity || "Other";';
    html += 'out += \'<div class="treemap-item" style="flex-basis:\' + size + \'%;background:\' + colors[idx % colors.length] + \'" onclick="filterByCommodity(\\x27\' + comm.replace(/\'/g, "\\\\\'") + \'\\x27)">\';';
    html += 'out += \'<div class="treemap-label">\' + comm + \'</div>\';';
    html += 'out += \'<div class="treemap-value">$\' + Math.round(value/1000).toLocaleString() + \'K</div>\';';
    html += 'out += \'<div class="treemap-pct">\' + pct.toFixed(1) + \'%</div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    // Weighted Average Price by Commodity chart
    html += 'out += \'<div class="dashboard-card"><h3>💰 Avg Cost/Unit by Commodity</h3>\';';
    html += 'var avgPriceData = commData.map(function(c) {';
    html += 'var qty = parseFloat(c.total_qty) || 0;';
    html += 'var dollars = parseFloat(c.total_dollars) || 0;';
    html += 'return { commodity: c.commodity || "Other", avgPrice: qty > 0 ? dollars / qty : 0, qty: qty, dollars: dollars };';
    html += '}).filter(function(d) { return d.avgPrice > 0; }).sort(function(a,b) { return b.avgPrice - a.avgPrice; });';
    html += 'var maxAvgPrice = avgPriceData.length > 0 ? Math.max.apply(null, avgPriceData.map(function(d) { return d.avgPrice; })) : 1;';
    html += 'out += \'<div style="display:flex;flex-direction:column;gap:6px;margin-top:0.75rem">\';';
    html += 'avgPriceData.slice(0,8).forEach(function(d, idx) {';
    html += 'var barWidth = maxAvgPrice > 0 ? (d.avgPrice / maxAvgPrice * 100) : 0;';
    html += 'out += \'<div style="display:flex;align-items:center;gap:8px">\';';
    html += 'out += \'<div style="width:70px;font-size:0.7rem;color:#1e3a5f;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="\' + d.commodity + \'">\' + d.commodity + \'</div>\';';
    html += 'out += \'<div style="flex:1;height:18px;background:#f0f0f0;border-radius:4px;overflow:hidden">\';';
    html += 'out += \'<div style="height:100%;width:\' + barWidth + \'%;background:\' + colors[idx % colors.length] + \';display:flex;align-items:center;justify-content:flex-end;padding-right:4px">\';';
    html += 'out += \'<span style="font-size:0.65rem;color:white;font-weight:600;text-shadow:0 1px 2px rgba(0,0,0,0.3)">$\' + d.avgPrice.toFixed(2) + \'</span>\';';
    html += 'out += \'</div></div>\';';
    html += 'out += \'<div style="width:50px;font-size:0.6rem;color:#86868b;text-align:right">\' + Math.round(d.qty/1000) + \'K u</div>\';';
    html += 'out += \'</div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += 'out += \'</div>\';'; // end dashboard-charts
    // Right side - top styles
    html += 'out += \'<div class="dashboard-products">\';';
    html += 'out += \'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;flex-wrap:wrap;gap:0.5rem">\';';
    html += 'out += \'<h3 style="margin:0;color:#1e3a5f">📦 Top Styles by PO Value <span style="font-size:0.75rem;color:#86868b;font-weight:normal">(showing \' + Math.min(items.length, state.poTilesLimit) + \' of \' + items.length + \')</span></h3>\';';
    html += 'out += \'<div style="display:flex;gap:0.5rem">\';';
    html += 'out += \'<button class="group-toggle-btn\' + (state.stackByStyle ? " active" : "") + \'" onclick="toggleStackByStylePO()">🎨 Stack Colors</button>\';';
    html += 'out += \'<button class="group-toggle-btn\' + (state.groupByVendor ? " active" : "") + \'" onclick="toggleGroupByVendor()">🏭 Group by Vendor</button>\';';
    html += 'out += \'</div></div>\';';
    // Grouped by vendor view
    html += 'if (state.groupByVendor) {';
    html += 'var vendorStyles = {};';
    html += 'var vendorTotals = {};';
    html += 'items.forEach(function(item) {';
    html += 'if (item.pos && item.pos.length > 0) { item.pos.forEach(function(po) {';
    html += 'var vend = po.vendor_name || "Unknown";';
    html += 'if (!vendorStyles[vend]) { vendorStyles[vend] = {}; vendorTotals[vend] = { dollars: 0, units: 0 }; }';
    html += 'if (!vendorStyles[vend][item.style_number]) { vendorStyles[vend][item.style_number] = { style_number: item.style_number, style_name: item.style_name, commodity: item.commodity, image_url: item.image_url, total_qty: 0, total_dollars: 0 }; }';
    html += 'vendorStyles[vend][item.style_number].total_qty += parseFloat(po.po_quantity) || 0;';
    html += 'vendorStyles[vend][item.style_number].total_dollars += parseFloat(po.po_total) || 0;';
    html += 'vendorTotals[vend].dollars += parseFloat(po.po_total) || 0;';
    html += 'vendorTotals[vend].units += parseFloat(po.po_quantity) || 0;';
    html += '}); }});';
    html += 'var sortedVendors = Object.keys(vendorTotals).sort(function(a,b) { return vendorTotals[b].dollars - vendorTotals[a].dollars; });';
    html += 'sortedVendors.forEach(function(vend) {';
    html += 'var styles = Object.values(vendorStyles[vend]).sort(function(a,b) { return b.total_dollars - a.total_dollars; });';
    html += 'var totals = vendorTotals[vend];';
    html += 'out += \'<div class="customer-group"><div class="customer-group-header" onclick="filterByVendor(\\x27\' + vend.replace(/\'/g, "\\\\\'") + \'\\x27)"><span class="customer-group-name">🏭 \' + vend + \'</span><span class="customer-group-stats">\' + styles.length + \' styles · \' + formatNumber(totals.units) + \' units · <span class="money">$\' + formatNumber(Math.round(totals.dollars)) + \'</span></span></div>\';';
    html += 'out += \'<div class="customer-group-grid">\';';
    html += 'styles.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc) { var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'out += \'<div class="mini-style-card">\';';
    html += 'out += \'<div class="mini-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:1.25rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="mini-style-info"><div class="mini-style-num">\' + item.style_number + \'</div><div class="mini-style-value">$\' + formatNumber(Math.round(item.total_dollars)) + \'</div></div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += '});';
    html += '} else {';
    // Normal view - with optional stacking by base style
    html += 'out += \'<div class="dashboard-grid">\';';
    html += 'if (state.stackByStyle) {';
    // Group items by base style
    html += 'var styleGroups = {};';
    html += 'items.slice(0, state.poTilesLimit).forEach(function(item) {';
    html += 'var baseStyle = item.style_number.split("-")[0];';
    html += 'if (!styleGroups[baseStyle]) styleGroups[baseStyle] = [];';
    html += 'styleGroups[baseStyle].push(item);';
    html += '});';
    // Render groups - stacked if multiple, normal if single
    html += 'var groupKeys = Object.keys(styleGroups).sort(function(a,b) {';
    html += 'var aTotal = styleGroups[a].reduce(function(s,i) { return s + (i.total_dollars || 0); }, 0);';
    html += 'var bTotal = styleGroups[b].reduce(function(s,i) { return s + (i.total_dollars || 0); }, 0);';
    html += 'return bTotal - aTotal; });';
    html += 'groupKeys.forEach(function(baseStyle) {';
    html += 'var group = styleGroups[baseStyle];';
    html += 'var topItem = group[0];';
    html += 'var imgSrc = topItem.image_url || "";';
    html += 'if (imgSrc) { var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'var totalDollars = group.reduce(function(s,i) { return s + (i.total_dollars || 0); }, 0);';
    html += 'var totalUnits = group.reduce(function(s,i) { return s + (i.total_qty || 0); }, 0);';
    html += 'var isExpanded = state.expandedStacks[baseStyle];';
    html += 'if (group.length > 1) {';
    // Render as stack
    html += 'out += \'<div class="style-stack\' + (isExpanded ? " expanded" : "") + \'" data-base="\' + baseStyle + \'" onclick="toggleStackPO(\\x27\' + baseStyle + \'\\x27, event)">\';';
    html += 'out += \'<span class="stack-badge">\' + group.length + \' colors</span>\';';
    html += 'out += \'<div class="dashboard-style-card">\';';
    html += 'out += \'<div class="dashboard-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:2rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info">\';';
    html += 'out += \'<div class="dashboard-style-name" style="font-weight:700">\' + baseStyle + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-num">\' + group.length + \' color variants</div>\';';
    html += 'out += \'<div class="dashboard-style-comm">\' + (topItem.commodity || "-") + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-stats"><span>\' + formatNumber(totalUnits) + \' units</span><span class="money">$\' + formatNumber(Math.round(totalDollars)) + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    // Expanded container with all variants
    html += 'out += \'<div class="stack-expanded-container">\';';
    html += 'out += \'<div class="stack-expanded-header"><span class="stack-expanded-title">🎨 \' + baseStyle + \' - \' + group.length + \' Colors</span><button class="stack-expanded-close" onclick="collapseStackPO(\\x27\' + baseStyle + \'\\x27, event)">×</button></div>\';';
    html += 'group.forEach(function(item) {';
    html += 'var iSrc = item.image_url || "";';
    html += 'if (iSrc) { var m = iSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (m) iSrc = "/api/image/" + m[1]; }';
    html += 'var fId = iSrc.startsWith("/api/image/") ? iSrc.replace("/api/image/", "") : "";';
    html += 'out += \'<div class="dashboard-style-card" onclick="showPOStyleDetail(\\x27\' + item.style_number + \'\\x27); event.stopPropagation();">\';';
    html += 'out += \'<div class="dashboard-style-img" style="height:100px">\';';
    html += 'if (iSrc) out += \'<img src="\' + iSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:1.5rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info" style="padding:0.5rem">\';';
    html += 'out += \'<div class="dashboard-style-num" style="font-size:0.7rem">\' + item.style_number + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-stats" style="font-size:0.65rem"><span>\' + formatNumber(item.total_qty || 0) + \' u</span><span class="money">$\' + formatNumber(Math.round(item.total_dollars || 0)) + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += '} else {';
    // Single item - render normally
    html += 'var item = topItem;';
    html += 'var poCount = item.po_count || (item.pos ? item.pos.length : 1);';
    html += 'out += \'<div class="dashboard-style-card" onclick="showPOStyleDetail(\\x27\' + item.style_number + \'\\x27)">\';';
    html += 'out += \'<div class="dashboard-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:2rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info">\';';
    html += 'out += \'<div class="dashboard-style-name">\' + (item.style_name || item.style_number) + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-num">\' + item.style_number + \' <span style="color:#86868b;font-size:0.625rem">\' + poCount + \' PO\' + (poCount !== 1 ? "s" : "") + \'</span></div>\';';
    html += 'out += \'<div class="dashboard-style-comm">\' + (item.commodity || "-") + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-stats"><span>\' + formatNumber(item.total_qty || 0) + \' units</span><span class="money">$\' + formatNumber(Math.round(item.total_dollars || 0)) + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    html += '}});';
    html += '} else {';
    // Original non-stacked view
    html += 'items.slice(0, state.poTilesLimit).forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc) { var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'var poCount = item.po_count || (item.pos ? item.pos.length : 1);';
    html += 'out += \'<div class="dashboard-style-card" onclick="showPOStyleDetail(\\x27\' + item.style_number + \'\\x27)">\';';
    html += 'out += \'<div class="dashboard-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:2rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info">\';';
    html += 'out += \'<div class="dashboard-style-name">\' + (item.style_name || item.style_number) + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-num">\' + item.style_number + \' <span style="color:#86868b;font-size:0.625rem">\' + poCount + \' PO\' + (poCount !== 1 ? "s" : "") + \'</span></div>\';';
    html += 'out += \'<div class="dashboard-style-comm">\' + (item.commodity || "-") + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-stats"><span>\' + formatNumber(item.total_qty || 0) + \' units</span><span class="money">$\' + formatNumber(Math.round(item.total_dollars || 0)) + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    html += '});';
    html += '}';
    html += 'out += \'</div>\';';
    // Load More button if there are more items
    html += 'if (items.length > state.poTilesLimit) {';
    html += 'out += \'<div style="text-align:center;padding:1.5rem"><button onclick="loadMorePOTiles()" style="background:#0088c2;color:white;border:none;padding:0.75rem 2rem;border-radius:980px;font-size:0.875rem;font-weight:600;cursor:pointer">Load More (\' + (items.length - state.poTilesLimit) + \' remaining)</button></div>\';';
    html += '}';
    html += '}';
    html += 'out += \'</div>\';';
    html += 'out += \'</div>\';'; // end dashboard-layout
    html += 'container.innerHTML = out; }';

    // PO Styles view
    html += 'function renderPOStyles(container, data) {';
    html += 'var items = data.items || [];';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No Import POs found</h3></div>\'; return; }';
    html += 'var html = \'<div class="product-grid">\';';
    html += 'items.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc) { var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'var poCount = item.po_count || (item.pos ? item.pos.length : 1);';
    html += 'html += \'<div class="style-card"><div class="style-image">\';';
    html += 'if (item.commodity) html += \'<span class="commodity-badge">\' + escapeHtml(item.commodity) + \'</span>\';';
    html += 'html += \'<span class="style-badge">\' + poCount + \' PO\' + (poCount > 1 ? "s" : "") + \'</span>\';';
    html += 'if (imgSrc) html += \'<img src="\' + imgSrc + \'" alt="" onerror="handleImgError(this,\\\'\' + fileId + \'\\\')\">\';';
    html += 'else html += \'<span style="color:#ccc;font-size:3rem">👔</span>\';';
    html += 'html += \'</div><div class="style-info"><div class="style-number">\' + escapeHtml(item.style_number) + \'</div>\';';
    html += 'html += \'<div class="style-name">\' + escapeHtml(item.style_name || item.style_number) + \'</div>\';';
    html += 'html += \'<div class="style-stats"><div class="style-stat"><div class="style-stat-value">\' + formatNumber(item.total_qty || 0) + \'</div><div class="style-stat-label">Units</div></div>\';';
    html += 'html += \'<div class="style-stat"><div class="style-stat-value money">\' + formatNumber(Math.round(item.total_dollars || 0)) + \'</div><div class="style-stat-label">Value</div></div></div></div></div>\'; });';
    html += 'html += \'</div>\'; container.innerHTML = html; }';

    // PO By Vendor view
    html += 'function renderPOByVendor(container, data) {';
    html += 'var items = data.items || [];';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No Import POs found</h3></div>\'; return; }';
    html += 'var vendorGroups = {};';
    html += 'items.forEach(function(item) {';
    html += 'if (!item.pos) return;';
    html += 'item.pos.forEach(function(po) {';
    html += 'var vendor = po.vendor_name || "Unknown";';
    html += 'if (!vendorGroups[vendor]) vendorGroups[vendor] = { totalQty: 0, totalDollars: 0, pos: {} };';
    html += 'var poNum = po.po_number || "Unknown";';
    html += 'if (!vendorGroups[vendor].pos[poNum]) vendorGroups[vendor].pos[poNum] = { items: [], totalQty: 0, totalDollars: 0, warehouseDate: po.po_warehouse_date };';
    html += 'vendorGroups[vendor].pos[poNum].items.push({ style: item.style_number, name: item.style_name, qty: po.po_quantity, total: po.po_total, image: item.image_url });';
    html += 'vendorGroups[vendor].pos[poNum].totalQty += po.po_quantity || 0;';
    html += 'vendorGroups[vendor].pos[poNum].totalDollars += po.po_total || 0;';
    html += 'vendorGroups[vendor].totalQty += po.po_quantity || 0;';
    html += 'vendorGroups[vendor].totalDollars += po.po_total || 0;';
    html += '}); });';
    html += 'var sortedVendors = Object.keys(vendorGroups).sort(function(a,b) { return vendorGroups[b].totalDollars - vendorGroups[a].totalDollars; });';
    html += 'var out = \'<div class="so-grid">\';';
    html += 'sortedVendors.forEach(function(vendor) {';
    html += 'var vg = vendorGroups[vendor];';
    html += 'var poNums = Object.keys(vg.pos).sort();';
    html += 'out += \'<div class="so-card"><div class="so-header"><div class="so-info"><h3>\' + vendor + \'</h3><span>\' + poNums.length + \' POs · \' + formatNumber(vg.totalQty) + \' units</span></div><div class="so-meta"><div class="total">$\' + formatNumber(Math.round(vg.totalDollars)) + \'</div></div></div>\';';
    html += 'poNums.forEach(function(poNum) {';
    html += 'var po = vg.pos[poNum];';
    html += 'var eta = po.warehouseDate ? new Date(po.warehouseDate).toLocaleDateString("en-US", {month: "short", day: "numeric"}) : "TBD";';
    html += 'out += \'<div style="padding:0.75rem 1rem;background:#f8fafc;border-top:1px solid #eee"><strong><a href="#" class="zoho-doc-link" data-type="purchaseorder" data-num="\' + escapeHtml(poNum) + \'" style="text-decoration:underline;color:inherit;cursor:pointer" title="Open in Zoho Inventory">PO# \' + poNum + \'</a></strong> <span style="color:#86868b;margin-left:1rem">ETA: \' + eta + \' · \' + po.items.length + \' styles · $\' + formatNumber(Math.round(po.totalDollars)) + \'</span></div>\';';
    html += '}); out += \'</div>\'; });';
    html += 'out += \'</div>\'; container.innerHTML = out; }';

    // PO Summary view (commodity x month matrix)
    html += 'function renderPOSummary(container, data) {';
    html += 'var items = data.items || [];';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No Import POs found</h3></div>\'; return; }';
    html += 'var commData = {}; var months = new Set();';
    html += 'items.forEach(function(item) {';
    html += 'var comm = item.commodity || "Other";';
    html += 'if (!commData[comm]) commData[comm] = { total: 0, months: {} };';
    html += 'if (item.pos) item.pos.forEach(function(po) {';
    html += 'var monthKey = po.po_warehouse_date ? po.po_warehouse_date.substring(0,7) : "TBD";';
    html += 'months.add(monthKey);';
    html += 'if (!commData[comm].months[monthKey]) commData[comm].months[monthKey] = 0;';
    html += 'commData[comm].months[monthKey] += po.po_total || 0;';
    html += 'commData[comm].total += po.po_total || 0;';
    html += '}); });';
    html += 'var sortedComms = Object.keys(commData).sort(function(a,b) { return commData[b].total - commData[a].total; });';
    html += 'var sortedMonths = Array.from(months).sort();';
    html += 'var monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];';
    html += 'var out = \'<div class="summary-container"><table class="summary-table"><thead><tr><th>Commodity</th>\';';
    html += 'sortedMonths.forEach(function(m) { if (m === "TBD") out += \'<th>TBD</th>\'; else { var parts = m.split("-"); out += \'<th>\' + monthNames[parseInt(parts[1])-1] + " \\x27" + parts[0].slice(2) + \'</th>\'; }});';
    html += 'out += \'<th class="row-total">Total</th></tr></thead><tbody>\';';
    html += 'sortedComms.forEach(function(comm) {';
    html += 'out += \'<tr><td>\' + comm + \'</td>\';';
    html += 'sortedMonths.forEach(function(m) {';
    html += 'var val = commData[comm].months[m] || 0;';
    html += 'out += \'<td><div class="summary-cell\' + (val > 0 ? " has-value" : "") + \'"><span class="dollars">\' + formatMoney(val) + \'</span></div></td>\';';
    html += '});';
    html += 'out += \'<td class="row-total"><div class="summary-cell has-value"><span class="dollars">\' + formatMoney(commData[comm].total) + \'</span></div></td></tr>\';';
    html += '});';
    html += 'out += \'</tbody></table></div>\';';
    html += 'container.innerHTML = out; }';

    // PO Price Compare view - compare pricing across vendors for same style
    html += 'function renderPOPriceCompare(container, data) {';
    html += 'var items = data.items || [];';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No Import POs found</h3></div>\'; return; }';
    // State for SKU vs Base Style toggle, variance filter, and actionable filter
    html += 'var mode = window._priceCompareMode || "sku";';
    html += 'var showVarianceOnly = window._priceCompareVarianceOnly || false;';
    html += 'var showActionableOnly = window._priceCompareActionableOnly || false;';
    // Calculate date boundaries for actionable window (±3 months)
    html += 'var today = new Date();';
    html += 'var threeMonthsAgo = new Date(today.getFullYear(), today.getMonth() - 3, today.getDate());';
    html += 'var threeMonthsAhead = new Date(today.getFullYear(), today.getMonth() + 3, today.getDate());';
    // Build grouped data
    html += 'var groups = {};';
    html += 'items.forEach(function(item) {';
    html += 'if (!item.pos) return;';
    html += 'item.pos.forEach(function(po) {';
    html += 'if (!po.po_unit_price && po.po_unit_price !== 0) return;';
    html += 'var key = mode === "base" ? (item.style_number || "").split("-")[0] : (item.style_name || item.style_number);';
    html += 'if (!key) return;';
    html += 'if (!groups[key]) groups[key] = { style: key, commodity: item.commodity || "-", entries: [], prices: new Set(), vendors: new Set(), totalQty: 0, totalDollars: 0, image: item.image_url, hasActionable: false, actionableQty: 0, actionableDollars: 0 };';
    html += 'var price = parseFloat(po.po_unit_price) || 0;';
    // Check if this PO is within actionable window
    html += 'var warehouseDate = po.po_warehouse_date ? new Date(po.po_warehouse_date) : null;';
    html += 'var isActionable = warehouseDate && warehouseDate >= threeMonthsAgo && warehouseDate <= threeMonthsAhead;';
    html += 'if (isActionable) { groups[key].hasActionable = true; groups[key].actionableQty += po.po_quantity || 0; groups[key].actionableDollars += po.po_total || 0; }';
    html += 'groups[key].entries.push({ vendor: po.vendor_name || "Unknown", po: po.po_number, price: price, qty: po.po_quantity || 0, total: po.po_total || 0, color: po.color || "", warehouse: po.po_warehouse_date, isActionable: isActionable });';
    html += 'groups[key].prices.add(price.toFixed(2));';
    html += 'groups[key].vendors.add(po.vendor_name || "Unknown");';
    html += 'groups[key].totalQty += po.po_quantity || 0;';
    html += 'groups[key].totalDollars += po.po_total || 0;';
    html += '}); });';
    // Calculate variance and sort
    html += 'var groupList = Object.values(groups).map(function(g) {';
    html += 'var priceArr = g.entries.map(function(e) { return e.price; });';
    html += 'g.minPrice = Math.min.apply(null, priceArr);';
    html += 'g.maxPrice = Math.max.apply(null, priceArr);';
    html += 'g.avgPrice = priceArr.reduce(function(a,b) { return a+b; }, 0) / priceArr.length;';
    html += 'g.variance = g.maxPrice - g.minPrice;';
    html += 'g.variancePct = g.minPrice > 0 ? (g.variance / g.minPrice * 100) : 0;';
    html += 'g.uniquePrices = g.prices.size;';
    html += 'g.uniqueVendors = g.vendors.size;';
    // Sort entries by price ascending within group
    html += 'g.entries.sort(function(a,b) { return a.price - b.price; });';
    html += 'return g; });';
    // Sort groups: actionable with variance first, then by variance desc, then by total dollars
    html += 'groupList.sort(function(a,b) {';
    html += 'var aActionableVariance = a.hasActionable && a.variance > 0 ? 1 : 0;';
    html += 'var bActionableVariance = b.hasActionable && b.variance > 0 ? 1 : 0;';
    html += 'if (bActionableVariance !== aActionableVariance) return bActionableVariance - aActionableVariance;';
    html += 'if (b.variance !== a.variance) return b.variance - a.variance;';
    html += 'return b.totalDollars - a.totalDollars; });';
    // Count styles with variance and calculate totals
    html += 'var withVariance = groupList.filter(function(g) { return g.variance > 0; }).length;';
    html += 'var totalVarianceDollars = groupList.filter(function(g) { return g.variance > 0; }).reduce(function(sum, g) { return sum + g.totalDollars; }, 0);';
    html += 'var maxVariancePct = groupList.length > 0 ? Math.max.apply(null, groupList.map(function(g) { return g.variancePct; })) : 0;';
    // Actionable stats
    html += 'var actionableWithVariance = groupList.filter(function(g) { return g.variance > 0 && g.hasActionable; }).length;';
    html += 'var actionableVarianceDollars = groupList.filter(function(g) { return g.variance > 0 && g.hasActionable; }).reduce(function(sum, g) { return sum + g.actionableDollars; }, 0);';
    // Filter list based on filters
    html += 'var displayList = groupList;';
    html += 'if (showVarianceOnly) displayList = displayList.filter(function(g) { return g.variance > 0; });';
    html += 'if (showActionableOnly) displayList = displayList.filter(function(g) { return g.hasActionable; });';
    // Build HTML
    html += 'var out = "";';
    // Summary cards at top
    html += 'out += \'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:1rem;margin-bottom:1.5rem">\';';
    // Card 1: Styles with variance
    html += 'out += \'<div style="background:linear-gradient(135deg,#fff5f5 0%,#fff 100%);border:1px solid #ffccc7;border-radius:12px;padding:1rem;border-left:4px solid #ff3b30">\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#86868b;text-transform:uppercase;letter-spacing:0.5px">Styles with Variance</div>\';';
    html += 'out += \'<div style="font-size:1.75rem;font-weight:700;color:#ff3b30;margin:0.25rem 0">\' + withVariance + \'</div>\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#6e6e73">of \' + groupList.length + \' total styles</div>\';';
    html += 'out += \'</div>\';';
    // Card 2: Actionable variances (key card!)
    html += 'out += \'<div style="background:linear-gradient(135deg,#fef3c7 0%,#fff 100%);border:1px solid #fde68a;border-radius:12px;padding:1rem;border-left:4px solid #f59e0b">\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#86868b;text-transform:uppercase;letter-spacing:0.5px">🔥 Actionable (±3mo)</div>\';';
    html += 'out += \'<div style="font-size:1.75rem;font-weight:700;color:#f59e0b;margin:0.25rem 0">\' + actionableWithVariance + \'</div>\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#6e6e73">styles you can affect</div>\';';
    html += 'out += \'</div>\';';
    // Card 3: Actionable value
    html += 'out += \'<div style="background:linear-gradient(135deg,#fff7ed 0%,#fff 100%);border:1px solid #fed7aa;border-radius:12px;padding:1rem;border-left:4px solid #ff9500">\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#86868b;text-transform:uppercase;letter-spacing:0.5px">Actionable Value</div>\';';
    html += 'out += \'<div style="font-size:1.75rem;font-weight:700;color:#ff9500;margin:0.25rem 0">$\' + formatNumber(Math.round(actionableVarianceDollars)) + \'</div>\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#6e6e73">within negotiation window</div>\';';
    html += 'out += \'</div>\';';
    // Card 4: Max variance
    html += 'out += \'<div style="background:linear-gradient(135deg,#f0f9ff 0%,#fff 100%);border:1px solid #bae6fd;border-radius:12px;padding:1rem;border-left:4px solid #0088c2">\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#86868b;text-transform:uppercase;letter-spacing:0.5px">Highest Variance</div>\';';
    html += 'out += \'<div style="font-size:1.75rem;font-weight:700;color:#0088c2;margin:0.25rem 0">\' + maxVariancePct.toFixed(0) + \'%</div>\';';
    html += 'out += \'<div style="font-size:0.75rem;color:#6e6e73">max price spread</div>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'</div>\';';
    // Header with toggles
    html += 'out += \'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;flex-wrap:wrap;gap:0.5rem">\';';
    html += 'out += \'<div><h3 style="margin:0;color:#1e3a5f">💰 Price Compare</h3>\';';
    html += 'out += \'<p style="margin:0.25rem 0 0;color:#86868b;font-size:0.8125rem">Click any row to see PO details · 🔥 = within ±3 months (actionable)</p></div>\';';
    // Toggle buttons container
    html += 'out += \'<div style="display:flex;gap:0.5rem;flex-wrap:wrap">\';';
    // Actionable Only toggle
    html += 'out += \'<button onclick="window._priceCompareActionableOnly=!\' + showActionableOnly + \';renderPOPriceCompare(document.getElementById(\\x27content\\x27),state.data)" style="padding:0.4rem 1rem;border:1px solid \' + (showActionableOnly ? "#f59e0b" : "#e0e0e0") + \';border-radius:980px;cursor:pointer;font-size:0.8125rem;font-weight:500;background:\' + (showActionableOnly ? "#fef3c7" : "#fff") + \';color:\' + (showActionableOnly ? "#d97706" : "#6e6e73") + \'">\' + (showActionableOnly ? "🔥 Actionable Only" : "All Dates") + \'</button>\';';
    // Variance Only toggle
    html += 'out += \'<button onclick="window._priceCompareVarianceOnly=!\' + showVarianceOnly + \';renderPOPriceCompare(document.getElementById(\\x27content\\x27),state.data)" style="padding:0.4rem 1rem;border:1px solid \' + (showVarianceOnly ? "#ff3b30" : "#e0e0e0") + \';border-radius:980px;cursor:pointer;font-size:0.8125rem;font-weight:500;background:\' + (showVarianceOnly ? "#fff5f5" : "#fff") + \';color:\' + (showVarianceOnly ? "#ff3b30" : "#6e6e73") + \'">\' + (showVarianceOnly ? "⚠️ Variance Only" : "All Prices") + \'</button>\';';
    // SKU / Base Style toggle
    html += 'out += \'<div style="display:flex;background:#f0f4f8;border-radius:980px;padding:3px">\';';
    html += 'out += \'<button onclick="window._priceCompareMode=\\x27sku\\x27;renderPOPriceCompare(document.getElementById(\\x27content\\x27),state.data)" style="padding:0.4rem 1rem;border:none;border-radius:980px;cursor:pointer;font-size:0.8125rem;font-weight:500;\' + (mode === "sku" ? "background:#1e3a5f;color:white" : "background:transparent;color:#6e6e73") + \'">SKU Level</button>\';';
    html += 'out += \'<button onclick="window._priceCompareMode=\\x27base\\x27;renderPOPriceCompare(document.getElementById(\\x27content\\x27),state.data)" style="padding:0.4rem 1rem;border:none;border-radius:980px;cursor:pointer;font-size:0.8125rem;font-weight:500;\' + (mode === "base" ? "background:#1e3a5f;color:white" : "background:transparent;color:#6e6e73") + \'">Base Style</button>\';';
    html += 'out += \'</div></div></div>\';';
    // Table
    html += 'out += \'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse;font-size:0.8125rem">\';';
    html += 'out += \'<thead><tr style="background:#f0f4f8;text-align:left">\';';
    html += 'out += \'<th style="padding:0.625rem 1rem;font-weight:600;color:#1e3a5f">Style</th>\';';
    html += 'out += \'<th style="padding:0.625rem 0.5rem;font-weight:600;color:#1e3a5f">Commodity</th>\';';
    html += 'out += \'<th style="padding:0.625rem 0.5rem;font-weight:600;color:#1e3a5f;text-align:center">Vendors</th>\';';
    html += 'out += \'<th style="padding:0.625rem 0.5rem;font-weight:600;color:#1e3a5f;min-width:180px">Price Range</th>\';';
    html += 'out += \'<th style="padding:0.625rem 0.5rem;font-weight:600;color:#1e3a5f;text-align:right">Variance</th>\';';
    html += 'out += \'<th style="padding:0.625rem 0.5rem;font-weight:600;color:#1e3a5f;text-align:right">Total Qty</th>\';';
    html += 'out += \'<th style="padding:0.625rem 1rem;font-weight:600;color:#1e3a5f;text-align:right">Total Value</th>\';';
    html += 'out += \'</tr></thead><tbody>\';';
    // Render each group
    html += 'displayList.forEach(function(g, idx) {';
    // Determine severity color
    html += 'var severity = g.variance === 0 ? "none" : g.variancePct > 20 ? "high" : g.variancePct > 10 ? "medium" : "low";';
    html += 'var borderColor = severity === "none" ? "transparent" : severity === "high" ? "#ff3b30" : severity === "medium" ? "#ff9500" : "#ffcc00";';
    html += 'var rowBg = severity === "none" ? (idx % 2 === 0 ? "#fff" : "#fafbfc") : severity === "high" ? "#fff5f5" : severity === "medium" ? "#fff7ed" : "#fffbeb";';
    // Group header row
    html += 'out += \'<tr style="background:\' + rowBg + \';border-left:4px solid \' + borderColor + \';cursor:pointer;transition:background 0.15s" onmouseover="this.style.background=\\x27#f0f7ff\\x27" onmouseout="this.style.background=\\x27\' + rowBg + \'\\x27" onclick="var el=this.nextSibling;while(el&&el.classList&&el.classList.contains(\\x27price-detail\\x27)){el.style.display=el.style.display===\\x27none\\x27?\\x27table-row\\x27:\\x27none\\x27;el=el.nextSibling;}">\';';
    html += 'out += \'<td style="padding:0.625rem 1rem;font-weight:600;color:#1e3a5f">\' + g.style + (g.hasActionable && g.variance > 0 ? \' <span title="Actionable - within ±3 months" style="font-size:0.75rem">🔥</span>\' : \'\') + (g.variance > 0 ? \' <span style="color:#ff9500;font-size:0.75rem">⚠️</span>\' : \'\') + \'</td>\';';
    html += 'out += \'<td style="padding:0.625rem 0.5rem;color:#6e6e73">\' + g.commodity + \'</td>\';';
    html += 'out += \'<td style="padding:0.625rem 0.5rem;text-align:center">\' + g.uniqueVendors + \'</td>\';';
    // Price range with visual bar
    html += 'out += \'<td style="padding:0.625rem 0.5rem">\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem">\';';
    html += 'out += \'<span style="color:#34c759;font-weight:600;min-width:55px">$\' + g.minPrice.toFixed(2) + \'</span>\';';
    // Visual price spread bar
    html += 'if (g.variance > 0) {';
    html += 'var barWidth = Math.min(100, Math.max(20, g.variancePct * 2));';
    html += 'out += \'<div style="flex:1;height:8px;background:#e5e7eb;border-radius:4px;position:relative;min-width:60px;max-width:100px">\';';
    html += 'out += \'<div style="position:absolute;left:0;top:0;height:100%;width:\' + barWidth + \'%;background:linear-gradient(90deg,#34c759 0%,#ff9500 50%,#ff3b30 100%);border-radius:4px"></div>\';';
    html += 'out += \'</div>\';';
    html += '} else {';
    html += 'out += \'<div style="flex:1;height:8px;background:#d1fae5;border-radius:4px;min-width:60px;max-width:100px"></div>\';';
    html += '}';
    html += 'out += \'<span style="color:\' + (g.variance > 0 ? "#ff3b30" : "#34c759") + \';font-weight:600;min-width:55px;text-align:right">$\' + g.maxPrice.toFixed(2) + \'</span>\';';
    html += 'out += \'</div></td>\';';
    // Variance with badge
    html += 'if (g.variance === 0) {';
    html += 'out += \'<td style="padding:0.625rem 0.5rem;text-align:right"><span style="background:#d1fae5;color:#059669;padding:0.2rem 0.5rem;border-radius:4px;font-size:0.75rem;font-weight:600">✓ Consistent</span></td>\';';
    html += '} else {';
    html += 'var badgeBg = severity === "high" ? "#fee2e2" : severity === "medium" ? "#ffedd5" : "#fef3c7";';
    html += 'var badgeColor = severity === "high" ? "#dc2626" : severity === "medium" ? "#ea580c" : "#d97706";';
    html += 'out += \'<td style="padding:0.625rem 0.5rem;text-align:right"><span style="background:\' + badgeBg + \';color:\' + badgeColor + \';padding:0.2rem 0.5rem;border-radius:4px;font-size:0.75rem;font-weight:600">$\' + g.variance.toFixed(2) + \' (\' + g.variancePct.toFixed(0) + \'%)</span></td>\';';
    html += '}';
    html += 'out += \'<td style="padding:0.625rem 0.5rem;text-align:right">\' + formatNumber(g.totalQty) + \'</td>\';';
    html += 'out += \'<td style="padding:0.625rem 1rem;text-align:right;font-weight:600">$\' + formatNumber(Math.round(g.totalDollars)) + \'</td>\';';
    html += 'out += \'</tr>\';';
    // Detail rows (collapsed by default, shown on click)
    html += 'g.entries.forEach(function(e, eIdx) {';
    html += 'var isLowest = e.price === g.minPrice;';
    html += 'var isHighest = e.price === g.maxPrice && g.variance > 0;';
    html += 'var detailBg = e.isActionable ? (isLowest && g.variance > 0 ? "#ecfdf5" : isHighest ? "#fef2f2" : "#fffbeb") : (isLowest && g.variance > 0 ? "#ecfdf5" : isHighest ? "#fef2f2" : "#f8fafc");';
    html += 'out += \'<tr class="price-detail" style="display:none;background:\' + detailBg + \';border-left:4px solid \' + borderColor + \'">\';';
    html += 'out += \'<td style="padding:0.5rem 1rem 0.5rem 2rem;font-size:0.8125rem">\';';
    // Show actionable badge
    html += 'if (e.isActionable) out += \'<span style="background:#fef3c7;color:#d97706;padding:0.1rem 0.35rem;border-radius:4px;font-size:0.65rem;font-weight:600;margin-right:0.35rem">🔥 ACTIVE</span>\';';
    html += 'out += \'<span style="font-weight:500;color:#1e3a5f"><a href="#" class="zoho-doc-link" data-type="purchaseorder" data-num="\' + escapeHtml(e.po) + \'" style="text-decoration:underline;color:inherit;cursor:pointer" title="Open in Zoho Inventory">PO# \' + e.po + \'</a></span>\';';
    html += 'if (e.color) out += \'<div style="font-size:0.75rem;color:#86868b">\' + e.color + \'</div>\';';
    // Show warehouse date
    html += 'if (e.warehouse) {';
    html += 'var wDate = new Date(e.warehouse);';
    html += 'var dateStr = wDate.toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" });';
    html += 'out += \'<div style="font-size:0.7rem;color:\' + (e.isActionable ? "#d97706" : "#86868b") + \'">ETA: \' + dateStr + \'</div>\';';
    html += '}';
    html += 'out += \'</td>\';';
    html += 'out += \'<td style="padding:0.5rem 0.5rem;font-size:0.8125rem;color:#6e6e73">\' + e.vendor + \'</td>\';';
    html += 'out += \'<td style="padding:0.5rem 0.5rem;text-align:center"></td>\';';
    // Price with position indicator
    html += 'out += \'<td style="padding:0.5rem 0.5rem">\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem">\';';
    html += 'if (isLowest && g.variance > 0) {';
    html += 'out += \'<span style="background:#d1fae5;color:#059669;padding:0.15rem 0.4rem;border-radius:4px;font-size:0.7rem;font-weight:600">▼ LOWEST</span>\';';
    html += '} else if (isHighest) {';
    html += 'out += \'<span style="background:#fee2e2;color:#dc2626;padding:0.15rem 0.4rem;border-radius:4px;font-size:0.7rem;font-weight:600">▲ HIGHEST</span>\';';
    html += '}';
    html += 'out += \'<span style="font-weight:600;color:\' + (isLowest && g.variance > 0 ? "#059669" : isHighest ? "#dc2626" : "#1e3a5f") + \';font-size:0.875rem">$\' + e.price.toFixed(2) + \'/unit</span>\';';
    html += 'out += \'</div></td>\';';
    html += 'out += \'<td style="padding:0.5rem 0.5rem;text-align:right;font-size:0.8125rem;color:#86868b">\';';
    html += 'if (g.variance > 0 && !isLowest) {';
    html += 'var diff = e.price - g.minPrice;';
    html += 'out += \'+$\' + diff.toFixed(2) + \' vs best\';';
    html += '}';
    html += 'out += \'</td>\';';
    html += 'out += \'<td style="padding:0.5rem 0.5rem;text-align:right;font-size:0.8125rem">\' + formatNumber(e.qty) + \'</td>\';';
    html += 'out += \'<td style="padding:0.5rem 1rem;text-align:right;font-size:0.8125rem;font-weight:500">$\' + formatNumber(Math.round(e.total)) + \'</td>\';';
    html += 'out += \'</tr>\';';
    html += '});';
    html += '});';
    html += 'out += \'</tbody></table></div>\';';
    // Legend at bottom
    html += 'out += \'<div style="margin-top:1rem;padding:0.75rem 1rem;background:#f8fafc;border-radius:8px;font-size:0.75rem">\';';
    html += 'out += \'<div style="display:flex;gap:1.5rem;flex-wrap:wrap;margin-bottom:0.5rem">\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem"><span style="width:12px;height:12px;background:#ff3b30;border-radius:2px"></span> High variance (&gt;20%)</div>\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem"><span style="width:12px;height:12px;background:#ff9500;border-radius:2px"></span> Medium variance (10-20%)</div>\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem"><span style="width:12px;height:12px;background:#ffcc00;border-radius:2px"></span> Low variance (&lt;10%)</div>\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem"><span style="width:12px;height:12px;background:#d1fae5;border-radius:2px"></span> Consistent pricing</div>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div style="display:flex;gap:1.5rem;flex-wrap:wrap;padding-top:0.5rem;border-top:1px solid #e5e7eb">\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem">🔥 <strong>Actionable</strong> = ETA within ±3 months (can still negotiate)</div>\';';
    html += 'out += \'<div style="display:flex;align-items:center;gap:0.5rem">📅 <strong>Historical</strong> = ETA outside window (reference only)</div>\';';
    html += 'out += \'</div></div>\';';
    html += 'container.innerHTML = out; }';

    // PO Charts view - dedicated charts page
    html += 'function renderPOCharts(container, data) {';
    html += 'var items = data.items || [];';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No Import POs found</h3></div>\'; return; }';
    html += 'var colors = ["#1e3a5f", "#0088c2", "#4da6d9", "#34c759", "#ff9500", "#ff3b30", "#af52de", "#5856d6", "#00c7be", "#86868b"];';
    html += 'var commData = data.commodityBreakdown || [];';
    html += 'var vendorData = data.vendorBreakdown || [];';
    html += 'var monthlyData = data.monthlyBreakdown || [];';
    html += 'var out = \'<div style="padding:1rem"><h2 style="color:#1e3a5f;margin-bottom:1.5rem">📈 Import PO Analytics</h2>\';';
    html += 'out += \'<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:1.5rem">\';';
    // Chart 1: Avg Cost/Unit by Commodity
    html += 'out += \'<div class="dashboard-card" style="padding:1.5rem"><h3 style="margin-bottom:1rem">💰 Avg Cost/Unit by Commodity</h3>\';';
    html += 'var avgByComm = commData.map(function(c) {';
    html += 'var qty = parseFloat(c.total_qty) || 0;';
    html += 'var dollars = parseFloat(c.total_dollars) || 0;';
    html += 'return { name: c.commodity || "Other", avgPrice: qty > 0 ? dollars / qty : 0, qty: qty, dollars: dollars };';
    html += '}).filter(function(d) { return d.avgPrice > 0; }).sort(function(a,b) { return b.avgPrice - a.avgPrice; });';
    html += 'var maxCommPrice = avgByComm.length > 0 ? Math.max.apply(null, avgByComm.map(function(d) { return d.avgPrice; })) : 1;';
    html += 'out += \'<div style="display:flex;flex-direction:column;gap:10px">\';';
    html += 'avgByComm.forEach(function(d, idx) {';
    html += 'var barWidth = maxCommPrice > 0 ? (d.avgPrice / maxCommPrice * 100) : 0;';
    html += 'out += \'<div style="display:flex;align-items:center;gap:12px">\';';
    html += 'out += \'<div style="width:100px;font-size:0.8rem;color:#1e3a5f;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="\' + d.name + \'">\' + d.name + \'</div>\';';
    html += 'out += \'<div style="flex:1;height:24px;background:#f0f0f0;border-radius:6px;overflow:hidden">\';';
    html += 'out += \'<div style="height:100%;width:\' + barWidth + \'%;background:\' + colors[idx % colors.length] + \';display:flex;align-items:center;justify-content:flex-end;padding-right:8px;min-width:60px">\';';
    html += 'out += \'<span style="font-size:0.75rem;color:white;font-weight:700">$\' + d.avgPrice.toFixed(2) + \'</span>\';';
    html += 'out += \'</div></div>\';';
    html += 'out += \'<div style="width:70px;font-size:0.7rem;color:#86868b;text-align:right">\' + Math.round(d.qty/1000) + \'K units</div>\';';
    html += 'out += \'</div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    // Chart 2: Avg Cost/Unit by Vendor
    html += 'out += \'<div class="dashboard-card" style="padding:1.5rem"><h3 style="margin-bottom:1rem">🏭 Avg Cost/Unit by Vendor</h3>\';';
    html += 'var avgByVendor = vendorData.map(function(v) {';
    html += 'var qty = parseFloat(v.total_qty) || 0;';
    html += 'var dollars = parseFloat(v.total_dollars) || 0;';
    html += 'return { name: v.vendor_name || "Unknown", avgPrice: qty > 0 ? dollars / qty : 0, qty: qty, dollars: dollars };';
    html += '}).filter(function(d) { return d.avgPrice > 0; }).sort(function(a,b) { return b.avgPrice - a.avgPrice; }).slice(0,10);';
    html += 'var maxVendorPrice = avgByVendor.length > 0 ? Math.max.apply(null, avgByVendor.map(function(d) { return d.avgPrice; })) : 1;';
    html += 'out += \'<div style="display:flex;flex-direction:column;gap:10px">\';';
    html += 'avgByVendor.forEach(function(d, idx) {';
    html += 'var barWidth = maxVendorPrice > 0 ? (d.avgPrice / maxVendorPrice * 100) : 0;';
    html += 'out += \'<div style="display:flex;align-items:center;gap:12px">\';';
    html += 'out += \'<div style="width:120px;font-size:0.75rem;color:#1e3a5f;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis" title="\' + d.name + \'">\' + d.name + \'</div>\';';
    html += 'out += \'<div style="flex:1;height:24px;background:#f0f0f0;border-radius:6px;overflow:hidden">\';';
    html += 'out += \'<div style="height:100%;width:\' + barWidth + \'%;background:\' + colors[idx % colors.length] + \';display:flex;align-items:center;justify-content:flex-end;padding-right:8px;min-width:60px">\';';
    html += 'out += \'<span style="font-size:0.75rem;color:white;font-weight:700">$\' + d.avgPrice.toFixed(2) + \'</span>\';';
    html += 'out += \'</div></div>\';';
    html += 'out += \'<div style="width:70px;font-size:0.7rem;color:#86868b;text-align:right">\' + Math.round(d.qty/1000) + \'K units</div>\';';
    html += 'out += \'</div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    // Chart 3: Monthly Value & Volume
    html += 'out += \'<div class="dashboard-card" style="padding:1.5rem"><h3 style="margin-bottom:1rem">📅 Monthly PO Value</h3>\';';
    html += 'var monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];';
    html += 'var maxMonthVal = monthlyData.length > 0 ? Math.max.apply(null, monthlyData.map(function(m) { return parseFloat(m.total_dollars) || 0; })) : 1;';
    html += 'out += \'<div style="display:flex;align-items:flex-end;gap:8px;height:200px;padding-top:20px">\';';
    html += 'monthlyData.forEach(function(m, idx) {';
    html += 'var val = parseFloat(m.total_dollars) || 0;';
    html += 'var barHeight = maxMonthVal > 0 ? (val / maxMonthVal * 100) : 0;';
    html += 'var parts = (m.month || "").split("-");';
    html += 'var label = parts.length === 2 ? monthNames[parseInt(parts[1])-1] + " \\x27" + parts[0].slice(2) : m.month;';
    html += 'out += \'<div style="flex:1;display:flex;flex-direction:column;align-items:center;height:100%">\';';
    html += 'out += \'<div style="flex:1;width:100%;display:flex;align-items:flex-end">\';';
    html += 'out += \'<div style="width:100%;height:\' + barHeight + \'%;background:\' + colors[idx % colors.length] + \';border-radius:4px 4px 0 0;min-height:4px" title="$\' + Math.round(val/1000) + \'K"></div>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div style="font-size:0.6rem;color:#86868b;margin-top:4px;white-space:nowrap">\' + label + \'</div>\';';
    html += 'out += \'<div style="font-size:0.6rem;color:#0088c2;font-weight:600">$\' + Math.round(val/1000) + \'K</div>\';';
    html += 'out += \'</div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    // Chart 4: Monthly Avg Price Trend
    html += 'out += \'<div class="dashboard-card" style="padding:1.5rem"><h3 style="margin-bottom:1rem">📊 Monthly Avg Cost/Unit Trend</h3>\';';
    html += 'var monthlyAvg = monthlyData.map(function(m) {';
    html += 'var qty = parseFloat(m.total_qty) || 0;';
    html += 'var dollars = parseFloat(m.total_dollars) || 0;';
    html += 'return { month: m.month, avgPrice: qty > 0 ? dollars / qty : 0 };';
    html += '}).filter(function(d) { return d.avgPrice > 0; });';
    html += 'var maxAvg = monthlyAvg.length > 0 ? Math.max.apply(null, monthlyAvg.map(function(d) { return d.avgPrice; })) : 1;';
    html += 'var minAvg = monthlyAvg.length > 0 ? Math.min.apply(null, monthlyAvg.map(function(d) { return d.avgPrice; })) : 0;';
    html += 'var range = maxAvg - minAvg || 1;';
    html += 'out += \'<div style="display:flex;align-items:flex-end;gap:8px;height:200px;padding-top:20px">\';';
    html += 'monthlyAvg.forEach(function(d, idx) {';
    html += 'var barHeight = range > 0 ? ((d.avgPrice - minAvg) / range * 80 + 20) : 50;';
    html += 'var parts = (d.month || "").split("-");';
    html += 'var label = parts.length === 2 ? monthNames[parseInt(parts[1])-1] + " \\x27" + parts[0].slice(2) : d.month;';
    html += 'out += \'<div style="flex:1;display:flex;flex-direction:column;align-items:center;height:100%">\';';
    html += 'out += \'<div style="flex:1;width:100%;display:flex;align-items:flex-end">\';';
    html += 'out += \'<div style="width:100%;height:\' + barHeight + \'%;background:linear-gradient(to top, #0088c2, #4da6d9);border-radius:4px 4px 0 0" title="$\' + d.avgPrice.toFixed(2) + \'"></div>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div style="font-size:0.6rem;color:#86868b;margin-top:4px;white-space:nowrap">\' + label + \'</div>\';';
    html += 'out += \'<div style="font-size:0.65rem;color:#0088c2;font-weight:600">$\' + d.avgPrice.toFixed(2) + \'</div>\';';
    html += 'out += \'</div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += 'out += \'</div></div>\';';
    html += 'container.innerHTML = out; }';

    // Render content
    html += 'function renderContent(data) {';
    html += 'var container = document.getElementById("content");';
    html += 'try {';
    html += 'if (state.view === "monthly") { renderMonthlyView(container, data.items || []); }';
    html += 'else if (state.view === "dashboard") { renderDashboardView(container, data); }';
    html += 'else if (state.view === "summary") { renderSummaryView(container, data.items || []); }';
    html += 'else if (state.view === "styles") { renderStylesView(container, data.items || []); }';
    html += 'else if (state.view === "topmovers") { renderTopMoversView(container, data.items || []); }';
    html += 'else if (state.view === "opportunities") { renderOpportunitiesView(container, data.items || []); }';
    html += 'else if (state.view === "charts") { renderChartsView(container, data); }';
    html += 'else if (state.view === "merchandising") { renderMerchandisingView(container, data); }';
    html += 'else { renderOrdersView(container, data.orders || []); }';
    html += '} catch(e) { console.error("Render error:", e); container.innerHTML = \'<div class="empty-state"><h3>Render Error</h3><p>\' + e.message + \'</p></div>\'; }}';

    // Render monthly view - group styles by delivery month, then by commodity
    html += 'function renderMonthlyView(container, items) {';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3><p>Try adjusting your filters or import some data</p></div>\'; return; }';
    // Build month groups with commodities
    html += 'var monthGroups = {};';
    html += 'var grandTotal = 0;';
    html += 'items.forEach(function(item) {';
    html += 'item.orders.forEach(function(o) {';
    html += 'var monthKey = o.delivery_date ? o.delivery_date.substring(0,7) : "9999-99";';
    html += 'var monthLabel = o.delivery_date ? formatMonthLabel(o.delivery_date.substring(0,7)) : "No Date";';
    html += 'var monthShort = o.delivery_date ? formatMonthShort(o.delivery_date.substring(0,7)) : "TBD";';
    html += 'var comm = item.commodity || "Other";';
    html += 'var orderStatus = o.status || "Open";';
    html += 'if (!monthGroups[monthKey]) monthGroups[monthKey] = { label: monthLabel, shortLabel: monthShort, commodities: {}, totalQty: 0, totalDollars: 0, openDollars: 0, invoicedDollars: 0, styleCount: 0 };';
    html += 'if (orderStatus === "Open" || orderStatus === "Partial") monthGroups[monthKey].openDollars += o.total_amount || 0;';
    html += 'else monthGroups[monthKey].invoicedDollars += o.total_amount || 0;';
    html += 'if (!monthGroups[monthKey].commodities[comm]) monthGroups[monthKey].commodities[comm] = { items: {}, totalQty: 0, totalDollars: 0 };';
    html += 'if (!monthGroups[monthKey].commodities[comm].items[item.style_number]) {';
    html += 'monthGroups[monthKey].commodities[comm].items[item.style_number] = { style_number: item.style_number, style_name: item.style_name, commodity: item.commodity, image_url: item.image_url, total_qty: 0, total_dollars: 0, order_count: 0 };';
    html += 'monthGroups[monthKey].styleCount++; }';
    html += 'monthGroups[monthKey].commodities[comm].items[item.style_number].total_qty += o.quantity || 0;';
    html += 'monthGroups[monthKey].commodities[comm].items[item.style_number].total_dollars += o.total_amount || 0;';
    html += 'monthGroups[monthKey].commodities[comm].items[item.style_number].order_count += 1;';
    html += 'monthGroups[monthKey].commodities[comm].totalQty += o.quantity || 0;';
    html += 'monthGroups[monthKey].commodities[comm].totalDollars += o.total_amount || 0;';
    html += 'monthGroups[monthKey].totalQty += o.quantity || 0;';
    html += 'monthGroups[monthKey].totalDollars += o.total_amount || 0;';
    html += 'grandTotal += o.total_amount || 0;';
    html += '}); });';
    html += 'var sortedMonths = Object.keys(monthGroups).sort();';
    // Build timeline bar with status colors
    html += 'var out = \'<div class="timeline-bar">\';';
    html += 'sortedMonths.forEach(function(mk, idx) {';
    html += 'var g = monthGroups[mk];';
    html += 'out += \'<div class="timeline-month" onclick="scrollToMonth(\\\'\' + mk + \'\\\')"><span>\' + g.shortLabel + \'</span>\';';
    html += 'out += \'<div class="tm-amounts">\';';
    html += 'if (g.openDollars > 0) out += \'<span class="tm-open">$\' + (g.openDollars/1000).toFixed(0) + \'K</span>\';';
    html += 'if (g.invoicedDollars > 0) out += \'<span class="tm-invoiced">$\' + (g.invoicedDollars/1000).toFixed(0) + \'K</span>\';';
    html += 'out += \'</div></div>\';';
    html += '});';
    html += 'out += \'</div>\';';
    // Build month sections
    html += 'sortedMonths.forEach(function(monthKey, mIdx) {';
    html += 'var group = monthGroups[monthKey];';
    html += 'var pct = grandTotal > 0 ? Math.round((group.totalDollars / grandTotal) * 100) : 0;';
    html += 'out += \'<div class="month-section" id="month-\' + monthKey + \'">\';';
    html += 'out += \'<div class="month-header" onclick="toggleMonth(\\\'\' + monthKey + \'\\\')">\';';
    html += 'out += \'<h2><span class="collapse-icon">▼</span>\' + group.label + \'</h2>\';';
    html += 'out += \'<div class="month-stats"><span>\' + group.styleCount + \' styles</span><span>\' + formatNumber(group.totalQty) + \' units</span><span class="money">$\' + formatNumber(Math.round(group.totalDollars)) + \'</span></div></div>\';';
    html += 'out += \'<div class="month-body" id="month-body-\' + monthKey + \'">\';';
    // Sort commodities by total dollars
    html += 'var sortedComms = Object.keys(group.commodities).sort(function(a,b) { return group.commodities[b].totalDollars - group.commodities[a].totalDollars; });';
    html += 'sortedComms.forEach(function(commName, cIdx) {';
    html += 'var commGroup = group.commodities[commName];';
    html += 'var commPct = group.totalDollars > 0 ? Math.round((commGroup.totalDollars / group.totalDollars) * 100) : 0;';
    html += 'var styleList = Object.values(commGroup.items).sort(function(a,b) { return b.total_dollars - a.total_dollars; });';
    html += 'var commClass = "comm-" + commName.toLowerCase().replace(/[^a-z]/g, "");';
    html += 'if (["top","dress","bottom","jacket","sweater"].indexOf(commName.toLowerCase()) === -1) commClass = "comm-other";';
    html += 'var commId = monthKey + "-" + commName.replace(/[^a-zA-Z0-9]/g, "");';
    html += 'out += \'<div class="commodity-section commodity-colors">\';';
    html += 'out += \'<div class="commodity-header \' + commClass + \'" onclick="toggleCommodity(\\\'\' + commId + \'\\\')">\';';
    html += 'out += \'<div class="commodity-header-left"><span class="collapse-icon">▼</span><h3>\' + escapeHtml(commName) + \'</h3></div>\';';
    html += 'out += \'<div class="commodity-header-right"><span class="comm-stat">\' + styleList.length + \' styles</span><span class="comm-stat">\' + formatNumber(commGroup.totalQty) + \' units</span><span class="comm-money">$\' + formatNumber(Math.round(commGroup.totalDollars)) + \'</span><span class="comm-stat">\' + commPct + \'%</span></div></div>\';';
    html += 'out += \'<div class="commodity-body" id="comm-body-\' + commId + \'"><div class="product-grid">\';';
    // Render style cards
    html += 'styleList.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc.includes("workdrive.zoho.com") || imgSrc.includes("download-accl.zoho.com")) {';
    html += 'var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'out += \'<div class="style-card" onclick="showStyleDetail(\\\'\'+ item.style_number +\'\\\')"><div class="style-image">\';';
    html += 'out += \'<span class="style-badge">\' + item.order_count + \' order\' + (item.order_count > 1 ? "s" : "") + \'</span>\';';
    html += 'var gm = calcGM(item); out += \'<span class="gm-badge \' + gm.cls + \'">\' + gm.label + \'</span>\';';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" onerror="handleImgError(this,\\\'\' + fileId + \'\\\')\">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:3rem">👔</span>\';';
    html += 'out += \'</div><div class="style-info"><div class="style-number">\' + escapeHtml(item.style_number) + \'</div>\';';
    html += 'out += \'<div class="style-name">\' + escapeHtml(item.style_name || item.style_number) + \'</div>\';';
    html += 'out += \'<div class="style-stats"><div class="style-stat"><div class="style-stat-value">\' + formatNumber(item.total_qty) + \'</div><div class="style-stat-label">Units</div></div>\';';
    html += 'out += \'<div class="style-stat"><div class="style-stat-value money">\' + formatNumber(Math.round(item.total_dollars)) + \'</div><div class="style-stat-label">Value</div></div></div></div></div>\'; });';
    html += 'out += \'</div></div></div>\'; });';
    html += 'out += \'</div></div>\'; });';
    html += 'container.innerHTML = out; }';

    // Render styles view
    html += 'function renderStylesView(container, items) {';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3><p>Try adjusting your filters or import some data</p></div>\'; return; }';
    html += 'var html = \'<div class="product-grid">\';';
    html += 'items.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc.includes("workdrive.zoho.com") || imgSrc.includes("download-accl.zoho.com")) {';
    html += 'var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'html += \'<div class="style-card" onclick="showStyleDetail(\\\'\'+ item.style_number +\'\\\')"><div class="style-image">\';';
    html += 'if (item.commodity) html += \'<span class="commodity-badge">\' + escapeHtml(item.commodity) + \'</span>\';';
    html += 'html += \'<span class="style-badge">\' + item.order_count + \' order\' + (item.order_count > 1 ? "s" : "") + \'</span>\';';
    html += 'var gm = calcGM(item); html += \'<span class="gm-badge \' + gm.cls + \'">\' + gm.label + \'</span>\';';
    html += 'if (imgSrc) html += \'<img src="\' + imgSrc + \'" alt="" onerror="handleImgError(this,\\\'\' + fileId + \'\\\')\">\';';
    html += 'else html += \'<span style="color:#ccc;font-size:3rem">👔</span>\';';
    html += 'html += \'</div><div class="style-info"><div class="style-number">\' + escapeHtml(item.style_number) + \'</div>\';';
    html += 'html += \'<div class="style-name">\' + escapeHtml(item.style_name || item.style_number) + \'</div>\';';
    html += 'html += \'<div class="style-stats"><div class="style-stat"><div class="style-stat-value">\' + formatNumber(item.total_qty) + \'</div><div class="style-stat-label">Units</div></div>\';';
    html += 'html += \'<div class="style-stat"><div class="style-stat-value money">\' + formatNumber(Math.round(item.total_dollars)) + \'</div><div class="style-stat-label">Value</div></div></div></div></div>\'; });';
    html += 'html += \'</div>\'; container.innerHTML = html; }';

    // Summary view toggle functions - using window for global access
    html += 'window.setSummaryGroupBy = function(groupBy) { state.summaryGroupBy = groupBy; state.expandedRows = {}; renderData(); };';
    html += 'window.toggleRowExpand = function(key) { if (state.expandedRows[key]) delete state.expandedRows[key]; else state.expandedRows[key] = true; renderData(); };';
    // Event delegation for summary view clicks
    html += 'document.addEventListener("click", function(e) {';
    html += 'var t = e.target;';
    html += 'if (t.dataset && t.dataset.groupby) { window.setSummaryGroupBy(t.dataset.groupby); return; }';
    html += 'if (t.dataset && t.dataset.expand) { window.toggleRowExpand(t.dataset.expand); return; }';
    html += 'if (t.parentElement && t.parentElement.dataset && t.parentElement.dataset.groupby) { window.setSummaryGroupBy(t.parentElement.dataset.groupby); return; }';
    html += 'if (t.parentElement && t.parentElement.dataset && t.parentElement.dataset.expand) { window.toggleRowExpand(t.parentElement.dataset.expand); return; }';
    html += '});';

    // Render summary view - matrix of commodities/customers vs months with toggle
    html += 'function renderSummaryView(container, items) {';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3><p>Try adjusting your filters or import some data</p></div>\'; return; }';
    html += 'var groupBy = state.summaryGroupBy || "commodity";';
    // Build data structures
    html += 'var mainData = {}; var breakdownData = {}; var months = new Set(); var primaryKeys = new Set(); var secondaryKeys = new Set();';
    html += 'items.forEach(function(item) {';
    html += 'var comm = item.commodity || "Other";';
    html += 'item.orders.forEach(function(o) {';
    html += 'var cust = o.customer || "Unknown";';
    html += 'var monthKey = o.delivery_date ? o.delivery_date.substring(0,7) : "TBD";';
    html += 'months.add(monthKey);';
    html += 'var primary = groupBy === "commodity" ? comm : cust;';
    html += 'var secondary = groupBy === "commodity" ? cust : comm;';
    html += 'primaryKeys.add(primary); secondaryKeys.add(secondary);';
    // Main aggregation
    html += 'var mainKey = primary + "|" + monthKey;';
    html += 'if (!mainData[mainKey]) mainData[mainKey] = { dollars: 0, units: 0 };';
    html += 'mainData[mainKey].dollars += o.total_amount || 0;';
    html += 'mainData[mainKey].units += o.quantity || 0;';
    // Breakdown aggregation
    html += 'var bkKey = primary + "|" + secondary + "|" + monthKey;';
    html += 'if (!breakdownData[bkKey]) breakdownData[bkKey] = { dollars: 0, units: 0 };';
    html += 'breakdownData[bkKey].dollars += o.total_amount || 0;';
    html += 'breakdownData[bkKey].units += o.quantity || 0;';
    html += '}); });';
    // Sort and calculate totals
    html += 'var sortedMonths = Array.from(months).sort();';
    html += 'var primaryTotals = {};';
    html += 'Array.from(primaryKeys).forEach(function(p) { primaryTotals[p] = { dollars: 0, units: 0 }; sortedMonths.forEach(function(m) { var k = p + "|" + m; if (mainData[k]) { primaryTotals[p].dollars += mainData[k].dollars; primaryTotals[p].units += mainData[k].units; } }); });';
    html += 'var sortedPrimary = Array.from(primaryKeys).sort(function(a,b) { return primaryTotals[b].dollars - primaryTotals[a].dollars; });';
    html += 'var colTotals = {}; sortedMonths.forEach(function(m) { colTotals[m] = { dollars: 0, units: 0 }; sortedPrimary.forEach(function(p) { var k = p + "|" + m; if (mainData[k]) { colTotals[m].dollars += mainData[k].dollars; colTotals[m].units += mainData[k].units; } }); });';
    html += 'var grandTotal = { dollars: 0, units: 0 }; sortedPrimary.forEach(function(p) { grandTotal.dollars += primaryTotals[p].dollars; grandTotal.units += primaryTotals[p].units; });';
    // Build HTML
    html += 'var out = \'<div class="summary-container">\';';
    // Toggle header
    html += 'out += \'<div class="summary-header"><div class="summary-toggle"><span>Group by:</span><div class="summary-toggle-btns">\';';
    html += 'out += \'<button class="summary-toggle-btn\' + (groupBy === "commodity" ? " active" : "") + \'" onclick="window.setSummaryGroupBy(this.textContent.toLowerCase())">Commodity</button>\';';
    html += 'out += \'<button class="summary-toggle-btn\' + (groupBy === "customer" ? " active" : "") + \'" onclick="window.setSummaryGroupBy(this.textContent.toLowerCase())">Customer</button>\';';
    html += 'out += \'</div></div>\';';
    html += 'if (groupBy === "commodity") { out += \'<div class="summary-legend"><span><div class="comm-color-box" style="background:#0088c2"></div>Top</span><span><div class="comm-color-box" style="background:#34c759"></div>Bottom</span><span><div class="comm-color-box" style="background:#af52de"></div>Dress</span><span><div class="comm-color-box" style="background:#ff9500"></div>Sweater</span><span><div class="comm-color-box" style="background:#ff3b30"></div>Jacket</span></div>\'; }';
    html += 'out += \'</div>\';';
    // Table
    html += 'out += \'<table class="summary-table"><thead><tr><th>\' + (groupBy === "commodity" ? "Commodity" : "Customer") + \'</th>\';';
    html += 'sortedMonths.forEach(function(m) { out += \'<th>\' + (m === "TBD" ? "TBD" : formatMonthShort(m)) + \'</th>\'; });';
    html += 'out += \'<th class="row-total">Total</th></tr></thead><tbody>\';';
    // Data rows with expand
    html += 'sortedPrimary.forEach(function(primary) {';
    html += 'var isExpanded = state.expandedRows[primary];';
    html += 'var rowClass = groupBy === "commodity" ? "comm-row-" + primary.toLowerCase().replace(/[^a-z]/g, "") : "";';
    html += 'if (groupBy === "commodity" && ["top","bottom","dress","sweater","jacket"].indexOf(primary.toLowerCase()) === -1) rowClass = "comm-row-other";';
    html += 'out += \'<tr class="\' + rowClass + \'"><td><button class="expand-btn\' + (isExpanded ? " expanded" : "") + \'" data-expand="\' + primary.replace(/["\']/g, "") + \'">▶</button>\' + escapeHtml(primary) + \'</td>\';';
    html += 'sortedMonths.forEach(function(m) { var k = primary + "|" + m; var cell = mainData[k] || { dollars: 0, units: 0 }; out += \'<td><div class="summary-cell\' + (cell.dollars > 0 ? " has-value" : "") + \'"><span class="dollars">\' + formatMoney(cell.dollars) + \'</span><span class="units">\' + formatNumber(cell.units) + \' units</span></div></td>\'; });';
    html += 'out += \'<td class="row-total"><div class="summary-cell has-value"><span class="dollars">\' + formatMoney(primaryTotals[primary].dollars) + \'</span><span class="units">\' + formatNumber(primaryTotals[primary].units) + \' units</span></div></td></tr>\';';
    // Expanded sub-rows
    html += 'if (isExpanded) { var secondaryTotals = {}; Array.from(secondaryKeys).forEach(function(s) { secondaryTotals[s] = { dollars: 0, units: 0 }; sortedMonths.forEach(function(m) { var bk = primary + "|" + s + "|" + m; if (breakdownData[bk]) { secondaryTotals[s].dollars += breakdownData[bk].dollars; secondaryTotals[s].units += breakdownData[bk].units; } }); });';
    html += 'var sortedSecondary = Array.from(secondaryKeys).filter(function(s) { return secondaryTotals[s].dollars > 0; }).sort(function(a,b) { return secondaryTotals[b].dollars - secondaryTotals[a].dollars; });';
    html += 'sortedSecondary.forEach(function(sec) { out += \'<tr class="summary-subrow"><td>\' + escapeHtml(sec) + \'</td>\';';
    html += 'sortedMonths.forEach(function(m) { var bk = primary + "|" + sec + "|" + m; var cell = breakdownData[bk] || { dollars: 0, units: 0 }; out += \'<td><div class="summary-cell\' + (cell.dollars > 0 ? " has-value" : "") + \'"><span class="dollars">\' + formatMoney(cell.dollars) + \'</span><span class="units">\' + formatNumber(cell.units) + \' units</span></div></td>\'; });';
    html += 'out += \'<td class="row-total"><div class="summary-cell has-value"><span class="dollars">\' + formatMoney(secondaryTotals[sec].dollars) + \'</span><span class="units">\' + formatNumber(secondaryTotals[sec].units) + \' units</span></div></td></tr>\'; }); }';
    html += '});';
    // Column totals
    html += 'out += \'<tr class="col-total"><td><strong>Total</strong></td>\';';
    html += 'sortedMonths.forEach(function(m) { out += \'<td><div class="summary-cell has-value"><span class="dollars">\' + formatMoney(colTotals[m].dollars) + \'</span><span class="units">\' + formatNumber(colTotals[m].units) + \' units</span></div></td>\'; });';
    html += 'out += \'<td class="grand-total"><div class="summary-cell"><span class="dollars" style="color:white">\' + formatMoney(grandTotal.dollars) + \'</span><span class="units" style="color:rgba(255,255,255,0.7)">\' + formatNumber(grandTotal.units) + \' units</span></div></td></tr>\';';
    html += 'out += \'</tbody></table></div>\';';
    html += 'container.innerHTML = out; }';

    // Helper: Get filter context string for display
    html += 'function getFilterContext() {';
    html += 'var parts = [];';
    html += 'if (state.filters.fiscalYears.length > 0) parts.push(state.filters.fiscalYears.length === 1 ? "FY" + state.filters.fiscalYears[0] : state.filters.fiscalYears.length + " fiscal years");';
    html += 'if (state.filters.years.length > 0) parts.push(state.filters.years.length === 1 ? state.filters.years[0] : state.filters.years.length + " years");';
    html += 'if (state.filters.months.length > 0) { var monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]; if (state.filters.months.length === 1) { var m = state.filters.months[0]; var p = m.split("-"); parts.push(monthNames[parseInt(p[1])-1] + " " + p[0]); } else { parts.push(state.filters.months.length + " months"); } }';
    html += 'if (state.filters.customers.length > 0) parts.push(state.filters.customers.length === 1 ? state.filters.customers[0] : state.filters.customers.length + " customers");';
    html += 'if (state.filters.commodities.length > 0) parts.push(state.filters.commodities.length === 1 ? state.filters.commodities[0] : state.filters.commodities.length + " commodities");';
    html += 'if (state.filters.status && state.filters.status !== "All") parts.push(state.filters.status + " orders");';
    html += 'return parts.length > 0 ? parts.join(" · ") : "All Data";';
    html += '}';

    // Helper: Render filter context header
    html += 'function renderFilterHeader(title, icon) {';
    html += 'return \'<div style="background:linear-gradient(135deg,#1e3a5f 0%,#0088c2 100%);color:white;padding:1rem 1.5rem;margin-bottom:1rem;border-radius:12px;display:flex;justify-content:space-between;align-items:center">\' +';
    html += '\'<div><span style="font-size:1.5rem;margin-right:0.5rem">\' + icon + \'</span><span style="font-size:1.25rem;font-weight:600">\' + title + \'</span></div>\' +';
    html += '\'<div style="background:rgba(255,255,255,0.2);padding:0.5rem 1rem;border-radius:8px;font-size:0.875rem">\' + getFilterContext() + \'</div></div>\';';
    html += '}';

    // Render Top Movers view - compact ranked table
    html += 'function renderTopMoversView(container, items) {';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3></div>\'; return; }';
    // Aggregate by style
    html += 'var styleData = {};';
    html += 'items.forEach(function(item) {';
    html += 'if (!styleData[item.style_number]) styleData[item.style_number] = { style_number: item.style_number, style_name: item.style_name, commodity: item.commodity, image_url: item.image_url, total_qty: 0, total_dollars: 0, order_count: 0, customers: new Set() };';
    html += 'item.orders.forEach(function(o) {';
    html += 'styleData[item.style_number].total_qty += o.quantity || 0;';
    html += 'styleData[item.style_number].total_dollars += o.total_amount || 0;';
    html += 'styleData[item.style_number].order_count++;';
    html += 'styleData[item.style_number].customers.add(o.customer);';
    html += '}); });';
    // Sort
    html += 'var sorted = Object.values(styleData);';
    html += 'if (state.sortBy === "units") sorted.sort(function(a,b) { return b.total_qty - a.total_qty; });';
    html += 'else if (state.sortBy === "orders") sorted.sort(function(a,b) { return b.order_count - a.order_count; });';
    html += 'else if (state.sortBy === "commodity") sorted.sort(function(a,b) { return (a.commodity || "ZZZ").localeCompare(b.commodity || "ZZZ"); });';
    html += 'else sorted.sort(function(a,b) { return b.total_dollars - a.total_dollars; });';
    // Render table
    html += 'var out = \'<div style="padding:1rem">\' + renderFilterHeader("Top Movers", "🏆");';
    html += 'out += \'<table class="topmovers-table" style="width:100%;border-collapse:collapse;font-size:0.875rem">\';';
    html += 'out += \'<thead><tr style="background:#f0f4f8;text-align:left"><th style="padding:0.75rem;width:50px">#</th><th style="padding:0.75rem">Style</th><th style="padding:0.75rem">Name</th><th style="padding:0.75rem">Commodity</th><th style="padding:0.75rem;text-align:right">Units</th><th style="padding:0.75rem;text-align:right">Value</th><th style="padding:0.75rem;text-align:center">Orders</th><th style="padding:0.75rem;text-align:center">Customers</th></tr></thead><tbody>\';';
    html += 'sorted.slice(0, 50).forEach(function(item, idx) {';
    html += 'var bgColor = idx % 2 === 0 ? "#fff" : "#f9fafb";';
    html += 'out += \'<tr style="background:\' + bgColor + \';border-bottom:1px solid #eee">\';';
    html += 'out += \'<td style="padding:0.75rem;font-weight:600;color:#86868b">\' + (idx + 1) + \'</td>\';';
    html += 'out += \'<td style="padding:0.75rem;font-weight:600"><a href="#" onclick="showStyleDetail(\\\'\' + item.style_number + \'\\\');return false" style="color:#1e3a5f;text-decoration:underline;cursor:pointer">\' + item.style_number + \'</a></td>\';';
    html += 'out += \'<td style="padding:0.75rem">\' + (item.style_name || "-") + \'</td>\';';
    html += 'out += \'<td style="padding:0.75rem"><span class="commodity-tag">\' + (item.commodity || "-") + \'</span></td>\';';
    html += 'out += \'<td style="padding:0.75rem;text-align:right;font-weight:500">\' + formatNumber(item.total_qty) + \'</td>\';';
    html += 'out += \'<td style="padding:0.75rem;text-align:right;font-weight:600;color:#0088c2">$\' + formatNumber(Math.round(item.total_dollars)) + \'</td>\';';
    html += 'out += \'<td style="padding:0.75rem;text-align:center">\' + item.order_count + \'</td>\';';
    html += 'out += \'<td style="padding:0.75rem;text-align:center;font-weight:500;color:\' + (item.customers.size > 2 ? "#34c759" : "#86868b") + \'">\' + item.customers.size + \'</td>\';';
    html += 'out += \'</tr>\'; });';
    html += 'out += \'</tbody></table></div>\';';
    html += 'container.innerHTML = out; }';

    // Render Opportunities view - multi-customer winners + who's missing
    html += 'function renderOpportunitiesView(container, items) {';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3></div>\'; return; }';
    // Get all customers and aggregate by commodity
    html += 'var allCustomers = new Set();';
    html += 'var commodityData = {};';
    html += 'items.forEach(function(item) {';
    html += 'var comm = item.commodity || "Other";';
    html += 'if (!commodityData[comm]) commodityData[comm] = { commodity: comm, total_dollars: 0, total_qty: 0, customers: new Set(), styles: [] };';
    html += 'item.orders.forEach(function(o) {';
    html += 'allCustomers.add(o.customer);';
    html += 'commodityData[comm].customers.add(o.customer);';
    html += 'commodityData[comm].total_dollars += o.total_amount || 0;';
    html += 'commodityData[comm].total_qty += o.quantity || 0;';
    html += '});';
    html += 'commodityData[comm].styles.push(item.style_number);';
    html += '});';
    // Sort commodities by value
    html += 'var sortedComms = Object.values(commodityData).sort(function(a,b) { return b.total_dollars - a.total_dollars; });';
    html += 'var customerList = Array.from(allCustomers).sort();';
    // Render
    html += 'var out = \'<div style="padding:1rem">\' + renderFilterHeader("Opportunity Finder", "🎯");';
    html += 'out += \'<p style="color:#86868b;margin-bottom:1.5rem">Commodities with multiple customers = proven winners. Customers missing = opportunity to sell!</p>\';';
    html += 'sortedComms.forEach(function(comm) {';
    html += 'var missingCustomers = customerList.filter(function(c) { return !comm.customers.has(c); });';
    html += 'var hasOpportunity = missingCustomers.length > 0 && comm.customers.size >= 2;';
    html += 'out += \'<div style="background:white;border-radius:12px;padding:1.5rem;margin-bottom:1rem;border:1px solid \' + (hasOpportunity ? "#34c759" : "#e5e5e5") + \'">\';';
    html += 'out += \'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem">\';';
    html += 'out += \'<div><h3 style="color:#1e3a5f;margin:0">\' + comm.commodity + \'</h3>\';';
    html += 'out += \'<span style="color:#86868b;font-size:0.875rem">\' + comm.styles.length + \' styles · $\' + formatNumber(Math.round(comm.total_dollars)) + \' · \' + formatNumber(comm.total_qty) + \' units</span></div>\';';
    html += 'out += \'<div style="font-size:1.5rem;font-weight:600;color:\' + (comm.customers.size >= 3 ? "#34c759" : comm.customers.size >= 2 ? "#ff9500" : "#86868b") + \'">\' + comm.customers.size + \' customers</div></div>\';';
    // Who's buying
    html += 'out += \'<div style="margin-bottom:0.75rem"><span style="font-weight:600;color:#1e3a5f;font-size:0.8125rem">✅ BUYING: </span>\';';
    html += 'out += Array.from(comm.customers).map(function(c) { return \'<span style="display:inline-block;background:#e8f5e9;color:#2e7d32;padding:0.25rem 0.5rem;border-radius:4px;margin:0.125rem;font-size:0.75rem">\' + c + \'</span>\'; }).join("");';
    html += 'out += \'</div>\';';
    // Who's NOT buying (opportunity)
    html += 'if (missingCustomers.length > 0 && comm.customers.size >= 2) {';
    html += 'out += \'<div><span style="font-weight:600;color:#ff3b30;font-size:0.8125rem">🎯 NOT BUYING (OPPORTUNITY): </span>\';';
    html += 'out += missingCustomers.map(function(c) { return \'<span style="display:inline-block;background:#ffebee;color:#c62828;padding:0.25rem 0.5rem;border-radius:4px;margin:0.125rem;font-size:0.75rem">\' + c + \'</span>\'; }).join("");';
    html += 'out += \'</div>\'; }';
    html += 'out += \'</div>\'; });';
    html += 'out += \'</div>\';';
    html += 'container.innerHTML = out; }';

    // Render orders view
    html += 'function renderOrdersView(container, orders) {';
    html += 'if (orders.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3><p>Try adjusting your filters or import some data</p></div>\'; return; }';
    html += 'var html = \'<div class="so-grid">\';';
    html += 'orders.forEach(function(order, idx) {';
    html += 'var deliveryDate = order.delivery_date ? new Date(order.delivery_date).toLocaleDateString("en-US", {month: "short", day: "numeric", year: "numeric"}) : "TBD";';
    html += 'html += \'<div class="so-card"><div class="so-header" onclick="toggleSO(\' + idx + \')">\';';
    html += 'html += \'<div class="so-info"><h3><a href="#" class="zoho-doc-link" data-type="salesorder" data-num="\' + escapeHtml(order.so_number) + \'" style="text-decoration:underline;color:inherit;cursor:pointer" title="Open in Zoho Inventory" onclick="event.stopPropagation()">SO# \' + escapeHtml(order.so_number) + \'</a></h3><span>\' + escapeHtml(order.customer) + \' · \' + order.line_count + \' items</span></div>\';';
    html += 'html += \'<div class="so-meta"><div class="delivery">\' + deliveryDate + \'</div><div class="total">$\' + formatNumber(Math.round(order.total_dollars)) + \'</div></div></div>\';';
    html += 'html += \'<div class="so-items" id="so-items-\' + idx + \'">\';';
    html += 'order.items.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc.includes("workdrive.zoho.com") || imgSrc.includes("download-accl.zoho.com")) {';
    html += 'var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'html += \'<div class="so-item"><div class="so-item-image">\';';
    html += 'if (imgSrc) html += \'<img src="\' + imgSrc + \'" alt="" onerror="handleImgError(this,\\\'\' + fileId + \'\\\')\">\';';
    html += 'else html += \'<span style="color:#ccc">👔</span>\';';
    html += 'html += \'</div><div class="so-item-info"><div class="so-item-style">\' + escapeHtml(item.style_number) + \'</div>\';';
    html += 'html += \'<div class="so-item-details">\' + escapeHtml(item.style_name || "") + (item.color ? " · " + escapeHtml(item.color) : "") + \'</div></div>\';';
    html += 'html += \'<div class="so-item-qty"><div class="qty">\' + formatNumber(item.quantity) + \' units</div><div class="amount">$\' + formatNumber(Math.round(item.total_amount)) + \'</div></div></div>\'; });';
    html += 'html += \'</div></div>\'; });';
    html += 'html += \'</div>\'; container.innerHTML = html; }';

    // Render charts view
    html += 'function renderChartsView(container, data) {';
    html += 'var items = data.items || [];';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No data for charts</h3><p>Import data to see visualizations</p></div>\'; return; }';
    html += 'var html = \'<button class="export-btn" onclick="exportToExcel()">📥 Export to Excel</button>\';';
    html += 'html += \'<div class="charts-container">\';';
    // Build chart data - monthly by commodity (for stacked bar)
    html += 'var monthlyData = {}; var customerData = {}; var commodityData = {}; var stackedData = {}; var colorData = {};';
    html += 'var allMonths = new Set(); var allCommodities = new Set();';
    html += 'items.forEach(function(item) {';
    html += 'var comm = item.commodity || "Other";';
    html += 'allCommodities.add(comm);';
    html += 'item.orders.forEach(function(o) {';
    html += 'var monthKey = o.delivery_date ? o.delivery_date.substring(0,7) : "9999-99";';
    html += 'allMonths.add(monthKey);';
    html += 'if (!monthlyData[monthKey]) monthlyData[monthKey] = { units: 0, dollars: 0 };';
    html += 'monthlyData[monthKey].units += o.quantity || 0;';
    html += 'monthlyData[monthKey].dollars += o.total_amount || 0;';
    html += 'if (!stackedData[monthKey]) stackedData[monthKey] = {};';
    html += 'if (!stackedData[monthKey][comm]) stackedData[monthKey][comm] = 0;';
    html += 'stackedData[monthKey][comm] += o.total_amount || 0;';
    html += 'var cust = o.customer || "Unknown";';
    html += 'if (!customerData[cust]) customerData[cust] = 0;';
    html += 'customerData[cust] += o.total_amount || 0;';
    html += 'if (!commodityData[comm]) commodityData[comm] = 0;';
    html += 'commodityData[comm] += o.total_amount || 0;';
    html += 'var color = o.color || "";';
    html += 'if (color) { if (!colorData[color]) colorData[color] = { dollars: 0, units: 0, styles: new Set() }; colorData[color].dollars += o.total_amount || 0; colorData[color].units += o.quantity || 0; colorData[color].styles.add(item.style_number); }';
    html += '}); });';
    // Sort months
    html += 'var sortedMonths = Array.from(allMonths).sort().filter(function(m) { return m !== "9999-99"; });';
    // Stacked bar chart placeholder
    html += 'html += \'<div class="chart-card" style="grid-column:span 2"><h3>📊 Monthly by Commodity (Stacked)</h3><div class="chart-wrapper" style="height:400px"><canvas id="stackedChart"></canvas></div></div>\';';
    // Treemap placeholder
    html += 'html += \'<div class="chart-card" style="grid-column:span 2"><h3>🗺️ Commodity Treemap</h3><div id="treemapContainer" style="height:400px;position:relative"></div></div>\';';
    // Customer pie chart placeholder
    html += 'html += \'<div class="chart-card"><h3>👥 Top Customers</h3><div class="chart-wrapper"><canvas id="customerChart"></canvas></div></div>\';';
    html += 'html += \'</div>\';';
    html += 'container.innerHTML = html;';
    // Render charts
    html += 'setTimeout(function() { renderCharts(monthlyData, customerData, commodityData, stackedData, sortedMonths, Array.from(allCommodities)); }, 100); }';

    // Render charts with Chart.js
    html += 'function renderCharts(monthlyData, customerData, commodityData, stackedData, sortedMonths, allCommodities) {';
    html += 'var colors = ["#1e3a5f", "#0088c2", "#4da6d9", "#34c759", "#ff9500", "#ff3b30", "#af52de", "#5856d6", "#00c7be", "#86868b", "#c7d1d9", "#2d5a87", "#66b3d9", "#003d5c"];';
    // Format month labels
    html += 'var monthLabels = sortedMonths.map(function(m) { var parts = m.split("-"); var months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]; return months[parseInt(parts[1])-1] + " " + parts[0].slice(2); });';
    // Sort commodities by total value
    html += 'var commSorted = Object.entries(commodityData).sort(function(a,b) { return b[1] - a[1]; }).map(function(e) { return e[0]; });';
    // Build stacked datasets
    html += 'var stackedDatasets = commSorted.map(function(comm, idx) {';
    html += 'return { label: comm, data: sortedMonths.map(function(m) { return stackedData[m] && stackedData[m][comm] ? Math.round(stackedData[m][comm]) : 0; }), backgroundColor: colors[idx % colors.length] };';
    html += '});';
    // Stacked bar chart
    html += 'new Chart(document.getElementById("stackedChart"), { type: "bar", data: { labels: monthLabels, datasets: stackedDatasets }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: "bottom", labels: { boxWidth: 12, font: { size: 10 } } } }, scales: { x: { stacked: true }, y: { stacked: true, beginAtZero: true, ticks: { callback: function(v) { return "$" + (v/1000).toFixed(0) + "K"; } } } } } });';
    // Treemap (using CSS boxes)
    html += 'var treemapContainer = document.getElementById("treemapContainer");';
    html += 'var total = Object.values(commodityData).reduce(function(a,b) { return a+b; }, 0);';
    html += 'var treemapHtml = "";';
    html += 'commSorted.forEach(function(comm, idx) {';
    html += 'var value = commodityData[comm];';
    html += 'var pct = (value / total * 100).toFixed(1);';
    html += 'var size = Math.max(Math.sqrt(value / total) * 100, 8);';
    html += 'treemapHtml += \'<div style="display:inline-block;width:\' + size + \'%;height:\' + size + \'%;min-width:60px;min-height:40px;background:\' + colors[idx % colors.length] + \';margin:2px;padding:8px;color:white;font-size:11px;border-radius:4px;overflow:hidden;vertical-align:top;box-sizing:border-box">\';';
    html += 'treemapHtml += \'<div style="font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis">\' + comm + \'</div>\';';
    html += 'treemapHtml += \'<div style="opacity:0.8">$\' + (value/1000).toFixed(0) + \'K</div>\';';
    html += 'treemapHtml += \'<div style="opacity:0.7;font-size:10px">\' + pct + \'%</div></div>\';';
    html += '});';
    html += 'treemapContainer.innerHTML = treemapHtml;';
    // Customer pie chart - top 6
    html += 'var custEntries = Object.entries(customerData).sort(function(a,b) { return b[1] - a[1]; }).slice(0, 6);';
    html += 'var custLabels = custEntries.map(function(e) { return e[0]; });';
    html += 'var custValues = custEntries.map(function(e) { return Math.round(e[1]); });';
    html += 'new Chart(document.getElementById("customerChart"), { type: "doughnut", data: { labels: custLabels, datasets: [{ data: custValues, backgroundColor: colors }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: "right" } } } });';
    html += '}';

    // Merchandising view
    html += 'var merchColors = ["#0088c2","#34c759","#ff9500","#ff3b30","#af52de","#5ac8fa","#ffcc00","#ff2d55","#8e8e93","#5856d6","#ff6961","#77dd77","#aec6cf","#fdfd96","#1e3a5f"];';

    // Color name to CSS color mapping for swatches
    html += 'function getColorSwatch(colorName) {';
    html += 'var name = (colorName || "").toUpperCase().trim();';
    html += 'var colorMap = {';
    html += '"PINK": "#FFB6C1", "HOT PINK": "#FF69B4", "LIGHT PINK": "#FFB6C1", "DUSTY PINK": "#D4A5A5", "BLUSH": "#DE5D83", "ROSE": "#FF007F", "CORAL PINK": "#F88379",';
    html += '"RED": "#DC143C", "BURGUNDY": "#800020", "WINE": "#722F37", "MAROON": "#800000", "CRIMSON": "#DC143C", "SCARLET": "#FF2400",';
    html += '"ORANGE": "#FF8C00", "CORAL": "#FF7F50", "PEACH": "#FFCBA4", "TANGERINE": "#FF9966", "RUST": "#B7410E", "BURNT ORANGE": "#CC5500",';
    html += '"YELLOW": "#FFD700", "GOLD": "#FFD700", "MUSTARD": "#FFDB58", "LEMON": "#FFF44F", "CREAM": "#FFFDD0", "IVORY": "#FFFFF0", "CHAMPAGNE": "#F7E7CE",';
    html += '"GREEN": "#228B22", "OLIVE": "#808000", "SAGE": "#9DC183", "MINT": "#98FF98", "LIME": "#32CD32", "EMERALD": "#50C878", "FOREST": "#228B22", "HUNTER": "#355E3B", "KELLY": "#4CBB17", "TEAL": "#008080", "SEAFOAM": "#93E9BE",';
    html += '"BLUE": "#4169E1", "NAVY": "#000080", "ROYAL": "#4169E1", "COBALT": "#0047AB", "SKY": "#87CEEB", "BABY BLUE": "#89CFF0", "POWDER BLUE": "#B0E0E6", "LIGHT BLUE": "#ADD8E6", "DENIM": "#1560BD", "INDIGO": "#4B0082",';
    html += '"PURPLE": "#800080", "LAVENDER": "#E6E6FA", "LILAC": "#C8A2C8", "VIOLET": "#8F00FF", "PLUM": "#DDA0DD", "MAUVE": "#E0B0FF", "MAGENTA": "#FF00FF", "FUCHSIA": "#FF00FF", "ORCHID": "#DA70D6", "EGGPLANT": "#614051",';
    html += '"BROWN": "#8B4513", "TAN": "#D2B48C", "BEIGE": "#F5F5DC", "KHAKI": "#C3B091", "CAMEL": "#C19A6B", "CHOCOLATE": "#7B3F00", "COFFEE": "#6F4E37", "TAUPE": "#483C32", "MOCHA": "#967969",';
    html += '"BLACK": "#1a1a1a", "CHARCOAL": "#36454F", "GREY": "#808080", "GRAY": "#808080", "SILVER": "#C0C0C0", "HEATHER": "#B6B095", "HEATHER GREY": "#9E9E9E", "ASH": "#B2BEB5",';
    html += '"WHITE": "#FFFFFF", "OFF WHITE": "#FAF9F6", "SNOW": "#FFFAFA", "NATURAL": "#FAF0E6", "OATMEAL": "#E6DDD1", "ECRU": "#C2B280",';
    html += '"LEOPARD": "#C69B5F", "ANIMAL": "#C69B5F", "CAMO": "#78866B", "CAMOUFLAGE": "#78866B", "TIE DYE": "linear-gradient(135deg, #ff6b6b, #feca57, #48dbfb, #ff9ff3)", "FLORAL": "#FF69B4", "STRIPE": "repeating-linear-gradient(90deg, #1a1a1a, #1a1a1a 3px, #fff 3px, #fff 6px)", "PLAID": "#8B0000"';
    html += '};';
    // Check for exact match first
    html += 'if (colorMap[name]) return colorMap[name];';
    // Check for partial matches (color name contains a key)
    html += 'for (var key in colorMap) {';
    html += 'if (name.indexOf(key) !== -1) return colorMap[key];';
    html += '}';
    // Check if key is in color name
    html += 'for (var key in colorMap) {';
    html += 'if (key.indexOf(name) !== -1 && name.length > 2) return colorMap[key];';
    html += '}';
    // Default fallback - use a neutral gray
    html += 'return "#B0B0B0";';
    html += '}';

    html += 'function renderMerchandisingView(container, data) {';
    html += 'var commodities = data.commodityBreakdown || [];';
    html += 'var customers = data.customerBreakdown || [];';
    html += 'var total = commodities.reduce(function(a,c) { return a + (parseFloat(c.total_dollars)||0); }, 0);';
    html += 'var totalUnits = commodities.reduce(function(a,c) { return a + (parseInt(c.total_qty)||0); }, 0);';

    // Build the HTML structure
    html += 'var out = \'<div class="merch-container">\';';

    // Section 1: Color Ranking (Top colors) - Most impactful first
    html += 'var colors = data.colorBreakdown || [];';
    html += 'var colorTotal = colors.reduce(function(a,c) { return a + (parseFloat(c.total_dollars)||0); }, 0);';
    html += 'out += \'<div class="merch-section"><h3>🎨 Top Colors by Value <span style="font-size:0.75rem;color:#86868b;font-weight:normal">(\' + colors.length + \' colors)</span></h3>\';';
    html += 'out += \'<div class="color-ranking-grid">\';';
    html += 'colors.forEach(function(c, idx) {';
    html += 'var pct = colorTotal > 0 ? (parseFloat(c.total_dollars)/colorTotal*100) : 0;';
    html += 'var barWidth = Math.max(pct * 4, 8);';
    html += 'out += \'<div class="color-rank-item">\';';
    html += 'out += \'<div class="color-rank-num">\' + (idx + 1) + \'</div>\';';
    html += 'out += \'<div class="color-rank-swatch" style="background:\' + getColorSwatch(c.color) + \'"></div>\';';
    html += 'out += \'<div class="color-rank-name">\' + (c.color || "Unknown") + \'</div>\';';
    html += 'out += \'<div class="color-rank-value">$\' + (parseFloat(c.total_dollars)/1000).toFixed(0) + \'K</div>\';';
    html += 'out += \'<div class="color-rank-meta">\' + parseInt(c.total_qty).toLocaleString() + \' units</div>\';';
    html += 'out += \'</div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';

    // Section 2: Commodity Mix Balance
    html += 'out += \'<div class="merch-section"><h3>📊 Commodity Mix Balance</h3><div class="merch-row">\';';
    html += 'out += \'<div class="merch-chart-box"><div class="merch-donut-wrapper"><canvas id="merchDonut" width="280" height="280"></canvas><div class="merch-donut-center"><div class="value">$\' + (total/1000000).toFixed(1) + \'M</div><div class="label">Total Value</div></div></div></div>\';';
    html += 'out += \'<div class="merch-legend">\';';
    html += 'commodities.forEach(function(c, idx) {';
    html += 'var pct = total > 0 ? (parseFloat(c.total_dollars)/total*100) : 0;';
    html += 'var badge = pct > 20 ? \'<span class="merch-legend-badge">OVER-INDEXED</span>\' : "";';
    html += 'out += \'<div class="merch-legend-item"><div class="merch-legend-color" style="background:\' + merchColors[idx % merchColors.length] + \'"></div><div class="merch-legend-info"><div class="merch-legend-name">\' + (c.commodity||"Other") + \'</div><div class="merch-legend-stats">$\' + (parseFloat(c.total_dollars)/1000).toFixed(0) + \'K (\' + pct.toFixed(1) + \'%) · \' + parseInt(c.total_qty).toLocaleString() + \' units</div></div>\' + badge + \'</div>\';';
    html += '});';
    html += 'out += \'</div></div></div>\';';

    // Section 3: Horizontal Bar Chart
    html += 'out += \'<div class="merch-section"><h3>📊 Commodities Ranked by Value</h3>\';';
    html += 'out += \'<div id="merchBarsContainer"></div></div>\';';

    // Section 4: Customer Scorecard
    html += 'out += \'<div class="merch-section"><h3>🎯 Customer Assortment Scorecard</h3>\';';
    html += 'out += \'<div class="customer-select-row"><label>Select Customer:</label><select id="scorecardCustomer" onchange="loadCustomerScorecard(this.value)"><option value="">— Choose a customer —</option>\';';
    html += 'customers.forEach(function(c) {';
    html += 'out += \'<option value="\' + (c.customer||"").replace(/"/g, "&quot;") + \'">\' + (c.customer||"Unknown") + \' ($\' + (parseFloat(c.total_dollars)/1000).toFixed(0) + \'K)</option>\';';
    html += '});';
    html += 'out += \'</select></div>\';';
    html += 'out += \'<div id="scorecardContent"><div style="text-align:center;padding:2rem;color:#86868b">Select a customer to view their commodity breadth</div></div></div>\';';

    html += 'out += \'</div>\';';
    html += 'container.innerHTML = out;';

    // Render charts
    html += 'setTimeout(function() { renderMerchDonut(commodities, total); renderMerchBars(commodities, total); }, 100);';
    html += '}';

    // Render donut chart function
    html += 'function renderMerchDonut(commodities, total) {';
    html += 'var canvas = document.getElementById("merchDonut");';
    html += 'if (!canvas) return;';
    html += 'var ctx = canvas.getContext("2d");';
    html += 'ctx.clearRect(0, 0, 280, 280);';
    html += 'var cx = 140, cy = 140, outerR = 120, innerR = 70;';
    html += 'var startAngle = -Math.PI / 2;';
    html += 'commodities.forEach(function(c, idx) {';
    html += 'var pct = (parseFloat(c.total_dollars) || 0) / total;';
    html += 'if (pct === 0) return;';
    html += 'var endAngle = startAngle + pct * 2 * Math.PI;';
    html += 'ctx.beginPath();';
    html += 'ctx.moveTo(cx + innerR * Math.cos(startAngle), cy + innerR * Math.sin(startAngle));';
    html += 'ctx.arc(cx, cy, outerR, startAngle, endAngle);';
    html += 'ctx.arc(cx, cy, innerR, endAngle, startAngle, true);';
    html += 'ctx.closePath();';
    html += 'ctx.fillStyle = merchColors[idx % merchColors.length];';
    html += 'ctx.fill();';
    html += 'startAngle = endAngle;';
    html += '});';
    html += '}';

    // Render horizontal bar chart function
    html += 'function renderMerchBars(commodities, total) {';
    html += 'var container = document.getElementById("merchBarsContainer");';
    html += 'if (!container) return;';
    html += 'var maxDollars = Math.max.apply(null, commodities.map(function(c) { return parseFloat(c.total_dollars) || 0; })) || 1;';
    html += 'var html = "";';
    html += 'commodities.forEach(function(c, idx) {';
    html += 'var dollars = parseFloat(c.total_dollars) || 0;';
    html += 'var units = parseInt(c.total_qty) || 0;';
    html += 'var pct = (dollars / maxDollars) * 100;';
    html += 'var mixPct = total > 0 ? (dollars / total * 100).toFixed(1) : 0;';
    html += 'html += \'<div style="display:flex;align-items:center;gap:1rem;margin-bottom:0.75rem">\';';
    html += 'html += \'<div style="width:100px;font-size:0.8125rem;font-weight:500;color:#1e3a5f;text-align:right;flex-shrink:0">\' + (c.commodity || "Other") + \'</div>\';';
    html += 'html += \'<div style="flex:1;background:#f0f4f8;border-radius:6px;height:28px;position:relative;overflow:hidden">\';';
    html += 'html += \'<div style="position:absolute;left:0;top:0;height:100%;width:\' + pct + \'%;background:\' + merchColors[idx % merchColors.length] + \';border-radius:6px;transition:width 0.3s"></div>\';';
    html += 'html += \'<div style="position:absolute;left:0;top:0;height:100%;width:100%;display:flex;align-items:center;padding:0 0.75rem;justify-content:space-between">\';';
    html += 'html += \'<span style="font-size:0.75rem;font-weight:600;color:\' + (pct > 40 ? \"#fff\" : \"#1e3a5f\") + \';z-index:1">$\' + (dollars/1000).toFixed(0) + \'K</span>\';';
    html += 'html += \'<span style="font-size:0.7rem;color:#86868b;z-index:1">\' + units.toLocaleString() + \' units · \' + mixPct + \'%</span>\';';
    html += 'html += \'</div></div></div>\';';
    html += '});';
    html += 'container.innerHTML = html;';
    html += '}';

    // Load customer scorecard
    html += 'function loadCustomerScorecard(customerName) {';
    html += 'var content = document.getElementById("scorecardContent");';
    html += 'if (!content) return;';
    html += 'if (!customerName) { content.innerHTML = \'<div style="text-align:center;padding:2rem;color:#86868b">Select a customer to view their commodity breadth</div>\'; return; }';
    html += 'content.innerHTML = \'<div style="text-align:center;padding:2rem;color:#86868b">Loading...</div>\';';
    // Calculate from state.data (the cached order data)
    html += 'if (!state.data || !state.data.items) { content.innerHTML = \'<div style="text-align:center;padding:2rem;color:#86868b">No data available</div>\'; return; }';
    html += 'var allCommodities = new Set();';
    html += 'var custCommodities = {};';
    html += 'var custTotal = { units: 0, dollars: 0, styles: 0 };';
    html += 'state.data.items.forEach(function(item) {';
    html += 'allCommodities.add(item.commodity || "Other");';
    html += 'item.orders.forEach(function(o) {';
    html += 'if (o.customer === customerName) {';
    html += 'var comm = item.commodity || "Other";';
    html += 'if (!custCommodities[comm]) custCommodities[comm] = { units: 0, dollars: 0 };';
    html += 'custCommodities[comm].units += o.quantity || 0;';
    html += 'custCommodities[comm].dollars += o.total_amount || 0;';
    html += 'custTotal.units += o.quantity || 0;';
    html += 'custTotal.dollars += o.total_amount || 0;';
    html += '}';
    html += '});';
    html += '});';
    html += 'var totalComms = allCommodities.size;';
    html += 'var custCommCount = Object.keys(custCommodities).length;';
    html += 'var breadth = totalComms > 0 ? Math.round((custCommCount / totalComms) * 100) : 0;';
    html += 'var healthClass = breadth >= 50 ? "strong" : (breadth >= 25 ? "moderate" : "opportunity");';
    html += 'var healthLabel = breadth >= 50 ? "Strong Breadth" : (breadth >= 25 ? "Moderate Breadth" : "Growth Opportunity");';
    // Build scorecard HTML
    html += 'var html = \'<div class="scorecard-grid">\';';
    html += 'html += \'<div class="scorecard-metric"><div class="value">\' + custCommCount + \'/\' + totalComms + \'</div><div class="label">Commodities</div></div>\';';
    html += 'html += \'<div class="scorecard-metric"><div class="value">\' + custTotal.units.toLocaleString() + \'</div><div class="label">Units</div></div>\';';
    html += 'html += \'<div class="scorecard-metric"><div class="value">$\' + (custTotal.dollars/1000).toFixed(0) + \'K</div><div class="label">Total Value</div></div>\';';
    html += 'html += \'</div>\';';
    html += 'html += \'<div style="text-align:center;margin:1rem 0"><div class="scorecard-health \' + healthClass + \'">\' + breadth + \'% Breadth - \' + healthLabel + \'</div></div>\';';
    // Top commodities
    html += 'var sortedComms = Object.entries(custCommodities).sort(function(a,b) { return b[1].dollars - a[1].dollars; }).slice(0, 5);';
    html += 'if (sortedComms.length > 0) {';
    html += 'html += \'<div class="scorecard-top-list"><h4 style="margin:0 0 0.75rem;font-size:0.875rem;color:#1e3a5f">Top Commodities</h4>\';';
    html += 'sortedComms.forEach(function(e) {';
    html += 'html += \'<div class="scorecard-top-item"><span>\' + e[0] + \'</span><span>\' + e[1].units.toLocaleString() + \' units · $\' + (e[1].dollars/1000).toFixed(0) + \'K</span></div>\';';
    html += '});';
    html += 'html += \'</div>\';';
    html += '}';
    html += 'content.innerHTML = html;';
    html += '}';

    // Dashboard view - hybrid charts + products
    html += 'function renderDashboardView(container, data) {';
    html += 'var items = data.items || [];';
    // Show clear buttons even when no data (so user can get back)
    html += 'if (items.length === 0) {';
    html += 'var emptyHtml = \'<div class="empty-state"><h3>No data found</h3><p>Try adjusting your filters</p>\';';
    html += 'if (state.filters.commodities.length > 0) emptyHtml += \'<button class="filter-clear-btn" style="margin-top:1rem" onclick="clearCommodityFilter()">✕ Clear commodities (\' + state.filters.commodities.length + \')</button>\';';
    html += 'if (state.filters.customers.length > 0) emptyHtml += \'<button class="filter-clear-btn" style="margin-top:0.5rem" onclick="clearCustomerFilter()">✕ Clear customer: \' + state.filters.customers[0] + \'</button>\';';
    html += 'if (state.filters.months.length > 0) emptyHtml += \'<button class="filter-clear-btn" style="margin-top:0.5rem" onclick="clearMonthFilter()">✕ Clear month filter</button>\';';
    html += 'emptyHtml += \'</div>\';';
    html += 'container.innerHTML = emptyHtml; return; }';
    // Use full breakdown data from API (not limited to 500 styles)
    html += 'var colors = ["#1e3a5f", "#0088c2", "#4da6d9", "#34c759", "#ff9500", "#ff3b30", "#af52de", "#5856d6", "#00c7be", "#86868b", "#c7d1d9", "#2d5a87", "#66b3d9", "#003d5c"];';
    // Commodity data from full breakdown
    html += 'var commSorted = (data.commodityBreakdown || []).map(function(r) { return [r.commodity || "Other", parseFloat(r.total_dollars) || 0]; });';
    html += 'var total = commSorted.reduce(function(a, e) { return a + e[1]; }, 0);';
    // Previous year commodity lookup for YoY
    html += 'var prevCommLookup = {};';
    html += '(data.prevYearCommodity || []).forEach(function(r) { prevCommLookup[r.commodity] = parseFloat(r.total_dollars) || 0; });';
    // Customer data from full breakdown
    html += 'var custSorted = (data.customerBreakdown || []).map(function(r) { return [r.customer || "Unknown", parseFloat(r.total_dollars) || 0]; });';
    // Previous year customer lookup for YoY
    html += 'var prevCustLookup = {};';
    html += '(data.prevYearCustomer || []).forEach(function(r) { prevCustLookup[r.customer] = parseFloat(r.total_dollars) || 0; });';
    // Missing customers
    html += 'var missingCustomers = data.missingCustomers || [];';
    // Monthly data from full breakdown
    html += 'var monthlyData = {};';
    html += '(data.monthlyBreakdown || []).forEach(function(r) {';
    html += 'if (r.month) monthlyData[r.month] = { dollars: parseFloat(r.total_dollars) || 0, units: parseFloat(r.total_qty) || 0 };';
    html += '});';
    html += 'var sortedMonths = Object.keys(monthlyData).sort().filter(function(m) { return m && m !== "9999-99"; });';
    html += 'var maxMonthValue = sortedMonths.length > 0 ? Math.max.apply(null, sortedMonths.map(function(m) { return monthlyData[m].dollars; })) : 1;';
    // Monthly by commodity for stacked bar
    html += 'var monthlyByComm = {};';
    html += '(data.monthlyByCommodity || []).forEach(function(r) {';
    html += 'if (!r.month || r.month === "9999-99") return;';
    html += 'if (!monthlyByComm[r.month]) monthlyByComm[r.month] = {};';
    html += 'monthlyByComm[r.month][r.commodity || "Other"] = parseFloat(r.total_dollars) || 0;';
    html += '});';
    // Get top commodities for legend (limit to 8)
    html += 'var topComms = commSorted.slice(0, 8).map(function(c) { return c[0]; });';
    // Sort items by value (these are still top 500 for display)
    html += 'var sortedItems = items.slice().sort(function(a,b) { return (b.total_dollars || 0) - (a.total_dollars || 0); });';
    // Build HTML
    html += 'var out = \'\';';
    // Month timeline - compact single row
    html += 'out += \'<div class="dashboard-timeline"><span class="timeline-title">📅 Months:</span><div class="timeline-bars">\';';
    html += 'var months = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];';
    html += 'var prevYear = null;';
    html += 'sortedMonths.forEach(function(monthKey, idx) {';
    html += 'var data = monthlyData[monthKey];';
    html += 'var parts = monthKey.split("-");';
    html += 'var year = parts[0];';
    html += 'var monthName = months[parseInt(parts[1])-1];';
    html += 'var nextKey = sortedMonths[idx + 1];';
    html += 'var nextYear = nextKey ? nextKey.split("-")[0] : null;';
    html += 'if (prevYear !== year) { out += \'<div class="timeline-year-group"><div class="timeline-year-label">\\x27\' + year.slice(-2) + \'</div><div class="timeline-year-months">\'; }';
    html += 'prevYear = year;';
    html += 'var isActive = state.filters.months.indexOf(monthKey) !== -1;';
    html += 'out += \'<div class="timeline-month\' + (isActive ? " active" : "") + \'" onclick="filterByMonth(\\x27\' + monthKey + \'\\x27)">\';';
    html += 'out += \'<div class="timeline-bar"><span class="bar-month">\' + monthName + \'</span></div>\';';
    html += 'out += \'<div class="timeline-stats"><span class="timeline-dollars">$\' + Math.round(data.dollars/1000).toLocaleString() + \'K</span></div></div>\';';
    html += 'if (nextYear !== year) { out += \'</div></div>\'; }';
    html += '});';
    html += 'out += \'</div>\';';
    html += 'if (state.filters.months.length > 0) { out += \'<button class="timeline-clear" onclick="clearMonthFilter()">✕</button>\'; }';
    html += 'out += \'</div>\';';
    // Main layout with collapsible sidebar
    html += 'var sidebarCollapsed = state.sidebarCollapsed || false;';
    html += 'out += \'<div class="dashboard-layout\' + (sidebarCollapsed ? " sidebar-collapsed" : "") + \'" id="dashboardLayout">\';';
    // Toggle button (only visible when collapsed - fixed position on left edge)
    html += 'out += \'<button class="sidebar-toggle" onclick="toggleDashboardSidebar()" title="Show filters">☰ Filters</button>\';';
    // Left column - charts
    html += 'out += \'<div class="dashboard-charts" id="dashboardSidebar">\';';
    // Mini stacked bar chart - only show when viewing multiple months (not when single month filtered)
    html += 'if (state.filters.months.length === 0) {';
    // Get months that actually have data in monthlyByComm, sorted - show ALL months, scrollable
    html += 'var now = new Date();';
    html += 'var currentMonth = now.getFullYear() + "-" + String(now.getMonth() + 1).padStart(2, "0");';
    html += 'var displayMonths = Object.keys(monthlyByComm).filter(function(m) {';
    html += 'var total = Object.values(monthlyByComm[m] || {}).reduce(function(a,v) { return a+v; }, 0);';
    html += 'return total > 0 && m && m !== "9999-99";';
    html += '}).sort();';
    html += 'var displayMax = displayMonths.length > 0 ? Math.max.apply(null, displayMonths.map(function(m) { var md = monthlyByComm[m] || {}; return Object.values(md).reduce(function(a,v) { return a+v; }, 0); })) : 1;';
    html += 'out += \'<div class="dashboard-card"><h3>📊 Monthly Trends <span class="sidebar-hide-link" onclick="toggleDashboardSidebar()">Hide «</span> <span style="float:right;font-size:0.75rem;color:#34c759;font-weight:600">$\' + (total/1000000).toFixed(1) + \'M total</span></h3>\';';
    // Build stacked bars in scrollable wrapper with arrow buttons
    html += 'out += \'<div class="mini-stacked-container">\';';
    html += 'out += \'<button class="mini-stacked-scroll-btn left" onclick="document.getElementById(\\x27stackedBarWrapper\\x27).scrollBy({left:-150,behavior:\\x27smooth\\x27})">◀</button>\';';
    html += 'out += \'<div class="mini-stacked-wrapper" id="stackedBarWrapper"><div class="mini-stacked">\';';
    html += 'var monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];';
    html += 'displayMonths.forEach(function(monthKey) {';
    html += 'var monthData = monthlyByComm[monthKey] || {};';
    html += 'var monthTotal = Object.values(monthData).reduce(function(a,v) { return a + v; }, 0);';
    html += 'var barHeight = displayMax > 0 ? (monthTotal / displayMax * 100) : 0;';
    html += 'var parts = monthKey.split("-");';
    html += 'var monthName = monthNames[parseInt(parts[1])-1] + " \\x27" + parts[0].slice(2);';
    html += 'out += \'<div class="mini-stacked-bar" style="height:100%">\';';
    html += 'out += \'<div style="height:\' + barHeight + \'%;display:flex;flex-direction:column;justify-content:flex-end">\';';
    // Build segments for each commodity
    html += 'topComms.forEach(function(comm, idx) {';
    html += 'var val = monthData[comm] || 0;';
    html += 'if (val > 0 && monthTotal > 0) {';
    html += 'var segPct = (val / monthTotal * 100).toFixed(1);';
    html += 'out += \'<div class="mini-stacked-segment" style="height:\' + segPct + \'%;background:\' + colors[idx] + \'" onclick="filterByCommodity(\\x27\' + comm.replace(/\'/g, "\\\\\'") + \'\\x27)" title="\' + comm + \': $\' + Math.round(val/1000) + \'K"></div>\';';
    html += '}});';
    // Add "Other" segment
    html += 'var otherVal = monthTotal - topComms.reduce(function(a,c) { return a + (monthData[c] || 0); }, 0);';
    html += 'if (otherVal > 0 && monthTotal > 0) {';
    html += 'var otherPct = (otherVal / monthTotal * 100).toFixed(1);';
    html += 'out += \'<div class="mini-stacked-segment" style="height:\' + otherPct + \'%;background:#c7d1d9" title="Other: $\' + Math.round(otherVal/1000) + \'K"></div>\';';
    html += '}';
    html += 'out += \'</div><div class="mini-stacked-label">\' + monthName + \'</div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';'; // Close mini-stacked and wrapper
    html += 'out += \'<button class="mini-stacked-scroll-btn right" onclick="document.getElementById(\\x27stackedBarWrapper\\x27).scrollBy({left:150,behavior:\\x27smooth\\x27})">▶</button>\';';
    html += 'out += \'</div>\';'; // Close container
    // Legend
    html += 'out += \'<div class="mini-stacked-legend">\';';
    html += 'topComms.slice(0,6).forEach(function(comm, idx) {';
    html += 'var val = commSorted.find(function(c) { return c[0] === comm; });';
    html += 'out += \'<div class="legend-item" onclick="filterByCommodity(\\x27\' + comm.replace(/\'/g, "\\\\\'") + \'\\x27)"><div class="legend-color" style="background:\' + colors[idx] + \'"></div>\' + comm + \'</div>\';';
    html += '});';
    html += 'out += \'</div>\';';
    html += 'out += \'</div>\';';
    html += '}'; // End of if (state.filters.months.length === 0)
    // Treemap (compact version) - Commodity only
    html += 'out += \'<div class="dashboard-card"><h3>🗺️ By Commodity <span style="font-size:0.75rem;color:#86868b">(click to filter)</span>\';';
    html += 'if (state.filters.months.length > 0) { out += \' <span class="sidebar-hide-link" onclick="toggleDashboardSidebar()">Hide «</span>\'; }';
    html += 'out += \' <span style="float:right;font-size:0.75rem;color:#34c759;font-weight:600">$\' + (total/1000000).toFixed(1) + \'M</span></h3><div class="dashboard-treemap">\';';
    html += 'commSorted.forEach(function(entry, idx) {';
    html += 'var comm = entry[0], value = entry[1];';
    html += 'var pct = (value / total * 100);';
    html += 'var width = Math.max(pct * 2.5, 18);'; // Scale percentage to width, min 18%
    html += 'out += \'<div class="treemap-item" style="flex-basis:\' + width + \'%;background:\' + colors[idx % colors.length] + \'" onclick="filterByCommodity(\\x27\' + comm.replace(/\'/g, "\\\\\'") + \'\\x27)">\';';
    html += 'out += \'<div class="treemap-label">\' + comm + \'</div>\';';
    html += 'out += \'<div class="treemap-value">$\' + Math.round(value/1000).toLocaleString() + \'K</div>\';';
    html += 'out += \'<div class="treemap-pct">\' + pct.toFixed(1) + \'%</div></div>\';';
    html += '});';
    html += 'out += \'</div>\';';
    html += 'if (state.filters.commodities.length > 0) { out += \'<button class="filter-clear-btn" onclick="clearCommodityFilter()">✕ Clear commodities (\' + state.filters.commodities.length + \')</button>\'; }';
    html += 'out += \'</div>\';';
    // Customer TY vs LY comparison tile (only show if YoY data exists)
    html += 'var hasYoYData = Object.keys(prevCustLookup).length > 0;';
    html += 'if (hasYoYData) {';
    html += 'out += \'<div class="dashboard-card"><h3>📊 Customer TY vs LY</h3><div class="yoy-list">\';';
    html += 'custSorted.slice(0, 8).forEach(function(entry) {';
    html += 'var cust = entry[0], tyValue = entry[1];';
    html += 'var lyValue = prevCustLookup[cust] || 0;';
    html += 'var change = lyValue > 0 ? ((tyValue - lyValue) / lyValue * 100) : (tyValue > 0 ? 100 : 0);';
    html += 'var changeClass = change > 0 ? "yoy-up" : (change < 0 ? "yoy-down" : "yoy-flat");';
    html += 'var changeIcon = change > 0 ? "▲" : (change < 0 ? "▼" : "–");';
    html += 'out += \'<div class="yoy-row"><div class="yoy-cust">\' + cust + \'</div>\';';
    html += 'out += \'<div class="yoy-values"><span class="yoy-ty">$\' + Math.round(tyValue/1000).toLocaleString() + \'K</span>\';';
    html += 'out += \'<span class="yoy-vs">vs</span>\';';
    html += 'out += \'<span class="yoy-ly">$\' + Math.round(lyValue/1000).toLocaleString() + \'K</span>\';';
    html += 'out += \'<span class="\' + changeClass + \'">\' + changeIcon + \' \' + Math.abs(change).toFixed(0) + \'%</span></div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += '}';
    // All customers (scrollable list)
    html += 'out += \'<div class="dashboard-card"><h3>👥 All Customers <span style="font-size:0.75rem;color:#86868b">(\' + custSorted.length + \' - click to filter)</span></h3><div class="dashboard-customers-scroll">\';';
    html += 'var custTotal = custSorted.reduce(function(a, e) { return a + e[1]; }, 0);';
    html += 'custSorted.forEach(function(entry, idx) {';
    html += 'var cust = entry[0], value = entry[1];';
    html += 'var pct = custTotal > 0 ? (value / custTotal * 100).toFixed(1) : 0;';
    html += 'out += \'<div class="customer-bar" onclick="filterByCustomer(\\x27\' + cust.replace(/\'/g, "\\\\\'") + \'\\x27)">\';';
    html += 'out += \'<div class="customer-name">\' + cust + \'</div>\';';
    html += 'out += \'<div class="customer-bar-fill" style="width:\' + pct + \'%;background:\' + colors[idx % colors.length] + \'"></div>\';';
    html += 'out += \'<div class="customer-value">$\' + Math.round(value/1000).toLocaleString() + \'K</div></div>\';';
    html += '});';
    html += 'out += \'</div>\';';
    html += 'if (state.filters.customers.length > 0) { out += \'<button class="filter-clear-btn" onclick="clearCustomerFilter()">✕ Clear: \' + state.filters.customers[0] + \'</button>\'; }';
    html += 'out += \'</div>\';';
    // Missing customers section
    html += 'if (missingCustomers.length > 0) {';
    html += 'var missingTotal = missingCustomers.reduce(function(a,c) { return a + (parseFloat(c.prev_year_dollars) || 0); }, 0);';
    html += 'out += \'<div class="dashboard-card missing-customers"><h3>⚠️ Missing Customers <span style="font-size:0.75rem;color:#ff3b30">$\' + Math.round(missingTotal/1000).toLocaleString() + \'K last year</span></h3>\';';
    html += 'out += \'<p style="font-size:0.75rem;color:#86868b;margin:0 0 0.5rem 0">Bought last FY but not this FY</p>\';';
    html += 'out += \'<div class="missing-list">\';';
    html += 'missingCustomers.slice(0,5).forEach(function(c) {';
    html += 'var lastOrder = c.last_order ? new Date(c.last_order).toLocaleDateString("en-US", {month:"short", year:"numeric"}) : "Unknown";';
    html += 'out += \'<div class="missing-item"><span class="missing-name">\' + c.customer + \'</span><span class="missing-details">Last: \' + lastOrder + \' · $\' + Math.round((parseFloat(c.prev_year_dollars)||0)/1000).toLocaleString() + \'K</span></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += '}';
    html += 'out += \'</div>\';'; // end dashboard-charts
    // Right column - top products
    html += 'out += \'<div class="dashboard-products">\';';
    html += 'out += \'<div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:1rem;flex-wrap:wrap;gap:0.5rem">\';';
    html += 'out += \'<h3 style="margin:0;color:#1e3a5f">📦 Top Styles by Value <span style="font-size:0.75rem;color:#86868b;font-weight:normal">(showing \' + sortedItems.length + \' of \' + (data.stats ? data.stats.style_count : sortedItems.length) + \')</span></h3>\';';
    html += 'out += \'<div style="display:flex;gap:0.5rem">\';';
    html += 'out += \'<button class="group-toggle-btn\' + (state.stackByStyle ? " active" : "") + \'" onclick="toggleStackByStyle()">🎨 Stack Colors</button>\';';
    html += 'out += \'<button class="group-toggle-btn\' + (state.groupByCustomer ? " active" : "") + \'" onclick="toggleGroupByCustomer()">👥 Group by Customer</button>\';';
    html += 'out += \'</div></div>\';';
    // Grouped by customer view
    html += 'if (state.groupByCustomer) {';
    html += 'var customerStyles = {};';
    html += 'var customerTotals = {};';
    html += 'sortedItems.forEach(function(item) {';
    html += 'if (item.orders) { item.orders.forEach(function(o) {';
    html += 'var cust = o.customer || "Unknown";';
    html += 'if (!customerStyles[cust]) { customerStyles[cust] = {}; customerTotals[cust] = { dollars: 0, units: 0 }; }';
    html += 'if (!customerStyles[cust][item.style_number]) { customerStyles[cust][item.style_number] = { style_number: item.style_number, style_name: item.style_name, commodity: item.commodity, image_url: item.image_url, total_qty: 0, total_dollars: 0 }; }';
    html += 'customerStyles[cust][item.style_number].total_qty += o.quantity || 0;';
    html += 'customerStyles[cust][item.style_number].total_dollars += o.total_amount || 0;';
    html += 'customerTotals[cust].dollars += o.total_amount || 0;';
    html += 'customerTotals[cust].units += o.quantity || 0;';
    html += '}); }});';
    html += 'var sortedCustomers = Object.keys(customerTotals).sort(function(a,b) { return customerTotals[b].dollars - customerTotals[a].dollars; });';
    html += 'sortedCustomers.forEach(function(cust) {';
    html += 'var styles = Object.values(customerStyles[cust]).sort(function(a,b) { return b.total_dollars - a.total_dollars; });';
    html += 'var totals = customerTotals[cust];';
    html += 'out += \'<div class="customer-group"><div class="customer-group-header" onclick="filterByCustomer(\\x27\' + cust.replace(/\'/g, "\\\\\'") + \'\\x27)"><span class="customer-group-name">👤 \' + cust + \'</span><span class="customer-group-stats">\' + styles.length + \' styles · \' + formatNumber(totals.units) + \' units · <span class="money">$\' + formatNumber(Math.round(totals.dollars)) + \'</span></span></div>\';';
    html += 'out += \'<div class="customer-group-grid">\';';
    html += 'styles.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc) { var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'out += \'<div class="mini-style-card" onclick="showStyleDetail(\\x27\' + item.style_number + \'\\x27)">\';';
    html += 'out += \'<div class="mini-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:1.25rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="mini-style-info"><div class="mini-style-num">\' + item.style_number + \'</div><div class="mini-style-value">$\' + formatNumber(Math.round(item.total_dollars)) + \'</div></div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += '});';
    html += '} else {';
    // Normal view - with optional stacking by base style
    html += 'out += \'<div class="dashboard-grid">\';';
    html += 'if (state.stackByStyle) {';
    // Group items by base style
    html += 'var styleGroups = {};';
    html += 'sortedItems.forEach(function(item) {';
    html += 'var baseStyle = item.style_number.split("-")[0];';
    html += 'if (!styleGroups[baseStyle]) styleGroups[baseStyle] = [];';
    html += 'styleGroups[baseStyle].push(item);';
    html += '});';
    // Render groups - stacked if multiple, normal if single
    html += 'var groupKeys = Object.keys(styleGroups).sort(function(a,b) {';
    html += 'var aTotal = styleGroups[a].reduce(function(s,i) { return s + (i.total_dollars || 0); }, 0);';
    html += 'var bTotal = styleGroups[b].reduce(function(s,i) { return s + (i.total_dollars || 0); }, 0);';
    html += 'return bTotal - aTotal; });';
    html += 'groupKeys.forEach(function(baseStyle) {';
    html += 'var group = styleGroups[baseStyle];';
    html += 'var topItem = group[0];';
    html += 'var imgSrc = topItem.image_url || "";';
    html += 'if (imgSrc) { var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'var totalDollars = group.reduce(function(s,i) { return s + (i.total_dollars || 0); }, 0);';
    html += 'var totalUnits = group.reduce(function(s,i) { return s + (i.total_qty || 0); }, 0);';
    html += 'var isExpanded = state.expandedStacks[baseStyle];';
    html += 'if (group.length > 1) {';
    // Render as stack
    html += 'out += \'<div class="style-stack\' + (isExpanded ? " expanded" : "") + \'" data-base="\' + baseStyle + \'" onclick="toggleStack(\\x27\' + baseStyle + \'\\x27, event)">\';';
    html += 'out += \'<span class="stack-badge">\' + group.length + \' colors</span>\';';
    html += 'out += \'<div class="dashboard-style-card">\';';
    html += 'out += \'<div class="dashboard-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:2rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info">\';';
    html += 'out += \'<div class="dashboard-style-name" style="font-weight:700">\' + baseStyle + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-num">\' + group.length + \' color variants</div>\';';
    html += 'out += \'<div class="dashboard-style-comm">\' + (topItem.commodity || "-") + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-stats"><span>\' + formatNumber(totalUnits) + \' units</span><span class="money">$\' + formatNumber(Math.round(totalDollars)) + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    // Expanded container with all variants
    html += 'out += \'<div class="stack-expanded-container">\';';
    html += 'out += \'<div class="stack-expanded-header"><span class="stack-expanded-title">🎨 \' + baseStyle + \' - \' + group.length + \' Colors</span><button class="stack-expanded-close" onclick="collapseStack(\\x27\' + baseStyle + \'\\x27, event)">×</button></div>\';';
    html += 'group.forEach(function(item) {';
    html += 'var iSrc = item.image_url || "";';
    html += 'if (iSrc) { var m = iSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (m) iSrc = "/api/image/" + m[1]; }';
    html += 'var fId = iSrc.startsWith("/api/image/") ? iSrc.replace("/api/image/", "") : "";';
    html += 'out += \'<div class="dashboard-style-card" onclick="showStyleDetail(\\x27\' + item.style_number + \'\\x27); event.stopPropagation();">\';';
    html += 'out += \'<div class="dashboard-style-img" style="height:100px">\';';
    html += 'if (iSrc) out += \'<img src="\' + iSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:1.5rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info" style="padding:0.5rem">\';';
    html += 'out += \'<div class="dashboard-style-num" style="font-size:0.7rem">\' + item.style_number + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-stats" style="font-size:0.65rem"><span>\' + formatNumber(item.total_qty || 0) + \' u</span><span class="money">$\' + formatNumber(Math.round(item.total_dollars || 0)) + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    html += '});';
    html += 'out += \'</div></div>\';';
    html += '} else {';
    // Single item - render normally
    html += 'var item = topItem;';
    html += 'var orderCount = item.order_count || (item.orders ? item.orders.length : 1);';
    html += 'var heatBadge = orderCount >= 3 ? "🔥" : (orderCount === 1 ? "❄️" : "");';
    html += 'out += \'<div class="dashboard-style-card" onclick="showStyleDetail(\\x27\' + item.style_number + \'\\x27)">\';';
    html += 'if (heatBadge) out += \'<div class="heat-badge">\' + heatBadge + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:2rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info">\';';
    html += 'out += \'<div class="dashboard-style-name">\' + (item.style_name || item.style_number) + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-num">\' + item.style_number + \' <span style="color:#86868b;font-size:0.625rem">\' + orderCount + \' order\' + (orderCount !== 1 ? "s" : "") + \'</span></div>\';';
    html += 'out += \'<div class="dashboard-style-comm">\' + (item.commodity || "-") + \'</div>\';';
    html += 'var gm = calcGM(item);';
    html += 'out += \'<div class="dashboard-style-stats"><span>\' + formatNumber(item.total_qty || 0) + \' units</span><span class="money">$\' + formatNumber(Math.round(item.total_dollars || 0)) + \'</span><span class="gm-badge \' + gm.cls + \'" style="position:static;margin-left:auto;font-size:0.6rem">\' + gm.label + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    html += '}});';
    html += '} else {';
    // Original non-stacked view
    html += 'sortedItems.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc) { var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'var fileId = imgSrc.startsWith("/api/image/") ? imgSrc.replace("/api/image/", "") : "";';
    html += 'var orderCount = item.order_count || (item.orders ? item.orders.length : 1);';
    html += 'var heatBadge = orderCount >= 3 ? "🔥" : (orderCount === 1 ? "❄️" : "");';
    html += 'out += \'<div class="dashboard-style-card" onclick="showStyleDetail(\\x27\' + item.style_number + \'\\x27)">\';';
    html += 'if (heatBadge) out += \'<div class="heat-badge">\' + heatBadge + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-img">\';';
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" loading="lazy" onerror="handleImgError(this,\\x27\' + fileId + \'\\x27)">\';';
    html += 'else out += \'<span style="color:#ccc;font-size:2rem">👔</span>\';';
    html += 'out += \'</div>\';';
    html += 'out += \'<div class="dashboard-style-info">\';';
    html += 'out += \'<div class="dashboard-style-name">\' + (item.style_name || item.style_number) + \'</div>\';';
    html += 'out += \'<div class="dashboard-style-num">\' + item.style_number + \' <span style="color:#86868b;font-size:0.625rem">\' + orderCount + \' order\' + (orderCount !== 1 ? "s" : "") + \'</span></div>\';';
    html += 'out += \'<div class="dashboard-style-comm">\' + (item.commodity || "-") + \'</div>\';';
    html += 'var gm = calcGM(item);';
    html += 'out += \'<div class="dashboard-style-stats"><span>\' + formatNumber(item.total_qty || 0) + \' units</span><span class="money">$\' + formatNumber(Math.round(item.total_dollars || 0)) + \'</span><span class="gm-badge \' + gm.cls + \'" style="position:static;margin-left:auto;font-size:0.6rem">\' + gm.label + \'</span></div>\';';
    html += 'out += \'</div></div>\';';
    html += '});';
    html += '}';
    html += 'out += \'</div>\';';
    html += '}';
    html += 'out += \'</div>\';'; // end dashboard-products
    html += 'out += \'</div>\';'; // end dashboard-layout
    html += 'container.innerHTML = out;';
    // Scroll stacked bar to current month
    html += 'setTimeout(function() {';
    html += 'var wrapper = document.getElementById("stackedBarWrapper");';
    html += 'if (wrapper) {';
    html += 'var now = new Date();';
    html += 'var currentMonth = now.getFullYear() + "-" + String(now.getMonth() + 1).padStart(2, "0");';
    html += 'var bars = wrapper.querySelectorAll(".mini-stacked-bar");';
    html += 'var targetIdx = -1;';
    html += 'bars.forEach(function(bar, idx) { var label = bar.querySelector(".mini-stacked-label"); if (label && label.textContent) { var txt = label.textContent; var monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"]; var mIdx = monthNames.findIndex(function(m) { return txt.startsWith(m); }); if (mIdx >= 0) { var yr = "20" + txt.slice(-2); var mk = yr + "-" + String(mIdx + 1).padStart(2, "0"); if (mk === currentMonth) targetIdx = idx; } } });';
    html += 'if (targetIdx > 0) { var scrollPos = Math.max(0, (targetIdx - 2) * 44); wrapper.scrollLeft = scrollPos; }';
    html += '}}, 100); }';

    // Filter by commodity from dashboard
    html += 'function filterByCommodity(comm) {';
    // Set the multi-select to just this commodity
    html += 'document.querySelectorAll("#commodityOptions input[type=checkbox]").forEach(function(cb) { cb.checked = cb.value === comm; });';
    html += 'state.filters.commodities = [comm];';
    html += 'document.getElementById("commodityDisplay").textContent = comm;';
    html += 'document.querySelector("#commodityMultiSelect .multi-select-display").classList.add("active");';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Filter by customer from dashboard
    html += 'function filterByCustomer(cust) {';
    html += 'var checkboxes = document.querySelectorAll("#customerOptions input[type=checkbox]");';
    html += 'checkboxes.forEach(function(cb) { cb.checked = cb.value === cust; });';
    html += 'state.filters.customers = [cust];';
    html += 'document.getElementById("customerDisplay").textContent = cust;';
    html += 'document.querySelector("#customerMultiSelect .multi-select-display").classList.add("active");';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Toggle dashboard sidebar
    html += 'function toggleDashboardSidebar() {';
    html += 'state.sidebarCollapsed = !state.sidebarCollapsed;';
    html += 'var layout = document.getElementById("dashboardLayout");';
    html += 'if (layout) layout.classList.toggle("sidebar-collapsed", state.sidebarCollapsed);';
    html += '}';

    // Toggle group by customer in dashboard
    html += 'function toggleGroupByCustomer() {';
    html += 'state.groupByCustomer = !state.groupByCustomer;';
    html += 'if (state.data) renderContent(state.data);';
    html += '}';

    // Toggle group by vendor in Import POs dashboard
    html += 'function toggleGroupByVendor() {';
    html += 'state.groupByVendor = !state.groupByVendor;';
    html += 'if (state.data) renderPOContent(state.data);';
    html += '}';

    // Toggle stack by style in dashboard
    html += 'function toggleStackByStyle() {';
    html += 'state.stackByStyle = !state.stackByStyle;';
    html += 'state.expandedStacks = {};'; // Reset expanded stacks
    html += 'if (state.data) renderContent(state.data);';
    html += '}';

    // Toggle expand/collapse a style stack
    html += 'function toggleStack(baseStyle, event) {';
    html += 'event.stopPropagation();';
    html += 'if (state.expandedStacks[baseStyle]) {';
    html += 'delete state.expandedStacks[baseStyle];';
    html += '} else {';
    html += 'state.expandedStacks = {};'; // Close others first
    html += 'state.expandedStacks[baseStyle] = true;';
    html += '}';
    html += 'if (state.data) renderContent(state.data);';
    html += '}';

    // Collapse a style stack (from close button)
    html += 'function collapseStack(baseStyle, event) {';
    html += 'event.stopPropagation();';
    html += 'delete state.expandedStacks[baseStyle];';
    html += 'if (state.data) renderContent(state.data);';
    html += '}';

    // PO-specific stack functions (call renderPOContent)
    html += 'function toggleStackByStylePO() {';
    html += 'state.stackByStyle = !state.stackByStyle;';
    html += 'state.expandedStacks = {};';
    html += 'if (state.data) renderPOContent(state.data);';
    html += '}';

    // Load more PO tiles
    html += 'function loadMorePOTiles() {';
    html += 'state.poTilesLimit += 200;';
    html += 'if (state.data) renderPOContent(state.data);';
    html += '}';

    html += 'function toggleStackPO(baseStyle, event) {';
    html += 'event.stopPropagation();';
    html += 'if (state.expandedStacks[baseStyle]) {';
    html += 'delete state.expandedStacks[baseStyle];';
    html += '} else {';
    html += 'state.expandedStacks = {};';
    html += 'state.expandedStacks[baseStyle] = true;';
    html += '}';
    html += 'if (state.data) renderPOContent(state.data);';
    html += '}';

    html += 'function collapseStackPO(baseStyle, event) {';
    html += 'event.stopPropagation();';
    html += 'delete state.expandedStacks[baseStyle];';
    html += 'if (state.data) renderPOContent(state.data);';
    html += '}';

    // Filter by vendor from Import POs dashboard (uses customer dropdown in PO mode)
    html += 'function filterByVendor(vend) {';
    html += 'var checkboxes = document.querySelectorAll("#customerOptions input[type=checkbox]");';
    html += 'checkboxes.forEach(function(cb) { cb.checked = cb.value === vend; });';
    html += 'state.filters.customers = [vend];';
    html += 'document.getElementById("customerDisplay").textContent = vend;';
    html += 'document.querySelector("#customerMultiSelect .multi-select-display").classList.add("active");';
    html += 'updateClearButton();';
    html += 'loadData(); }';


    // Filter by month from dashboard timeline
    html += 'function filterByMonth(monthKey) {';
    // Set the multi-select to just this month
    html += 'document.querySelectorAll("#monthOptions input[type=checkbox]").forEach(function(cb) { cb.checked = cb.value === monthKey; });';
    html += 'state.filters.months = [monthKey];';
    html += 'document.getElementById("monthDisplay").textContent = monthKey;';
    html += 'document.querySelector("#monthMultiSelect .multi-select-display").classList.add("active");';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Filter by month from Import POs timeline (toggle behavior)
    html += 'function filterByMonthPO(monthKey) {';
    html += 'var idx = state.filters.months.indexOf(monthKey);';
    html += 'if (idx === -1) { state.filters.months = [monthKey]; }';  // Select this month
    html += 'else { state.filters.months = []; }';  // Deselect if already selected
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Clear month filter from dashboard
    html += 'function clearMonthFilter() {';
    html += 'document.querySelectorAll("#monthOptions input[type=checkbox]").forEach(function(cb) { cb.checked = false; });';
    html += 'state.filters.months = [];';
    html += 'document.getElementById("monthDisplay").textContent = "All Months";';
    html += 'document.querySelector("#monthMultiSelect .multi-select-display").classList.remove("active");';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Clear commodity filter from dashboard
    html += 'function clearCommodityFilter() {';
    html += 'document.querySelectorAll("#commodityOptions input[type=checkbox]").forEach(function(cb) { cb.checked = false; });';
    html += 'state.filters.commodities = [];';
    html += 'document.getElementById("commodityDisplay").textContent = "All Commodities";';
    html += 'document.querySelector("#commodityMultiSelect .multi-select-display").classList.remove("active");';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Clear customer filter from dashboard
    html += 'function clearCustomerFilter() {';
    html += 'document.querySelectorAll("#customerOptions input[type=checkbox]").forEach(function(cb) { cb.checked = false; });';
    html += 'state.filters.customers = [];';
    html += 'document.getElementById("customerDisplay").textContent = "All Customers";';
    html += 'document.querySelector("#customerMultiSelect .multi-select-display").classList.remove("active");';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Generic multi-select helper functions
    html += 'function populateMultiSelect(type, items, defaultLabel) {';
    html += 'var dropdown = document.getElementById(type + "Dropdown");';
    html += 'dropdown.innerHTML = \'<div class="multi-select-search"><input type="text" placeholder="Search..." oninput="filterMultiSelectOptions(\\\'\' + type + \'\\\', this.value)"></div>\';';
    html += 'dropdown.innerHTML += \'<div class="multi-select-actions"><button class="select-all" onclick="selectAllFilter(\\\'\' + type + \'\\\')">All</button><button class="clear-all" onclick="clearAllFilter(\\\'\' + type + \'\\\')">Clear</button></div>\';';
    html += 'dropdown.innerHTML += \'<div class="multi-select-options" id="\' + type + \'Options"></div>\';';
    html += 'var optionsContainer = document.getElementById(type + "Options");';
    html += 'items.forEach(function(item) {';
    html += 'var div = document.createElement("div");';
    html += 'div.className = "multi-select-option";';
    html += 'div.setAttribute("data-value", item.value);';
    html += 'div.setAttribute("data-label", item.label.toLowerCase());';
    html += 'div.innerHTML = \'<input type="checkbox" value="\' + item.value + \'" onchange="updateMultiFilter(\\\'\' + type + \'\\\', \\\'\' + defaultLabel + \'\\\')"><span>\' + item.label + \'</span>\';';
    html += 'optionsContainer.appendChild(div);';
    html += '}); }';

    html += 'function toggleMultiSelect(type) {';
    html += 'var dropdown = document.getElementById(type + "Dropdown");';
    html += 'var isOpen = dropdown.classList.contains("open");';
    html += 'document.querySelectorAll(".multi-select-dropdown.open").forEach(function(d) { d.classList.remove("open"); });';
    html += 'if (!isOpen) { dropdown.classList.add("open"); var searchInput = dropdown.querySelector("input[type=text]"); if (searchInput) searchInput.focus(); }';
    html += 'setTimeout(function() { document.addEventListener("click", function closeDropdown(e) { if (!e.target.closest(".multi-select")) { dropdown.classList.remove("open"); document.removeEventListener("click", closeDropdown); } }); }, 10); }';

    html += 'function filterMultiSelectOptions(type, query) {';
    html += 'var options = document.querySelectorAll("#" + type + "Options .multi-select-option");';
    html += 'var lowerQuery = query.toLowerCase();';
    html += 'options.forEach(function(opt) {';
    html += 'var label = opt.getAttribute("data-label") || "";';
    html += 'opt.classList.toggle("hidden", lowerQuery && label.indexOf(lowerQuery) === -1);';
    html += '}); }';

    html += 'function updateMultiFilter(type, defaultLabel) {';
    html += 'var checkboxes = document.querySelectorAll("#" + type + "Options input[type=checkbox]:checked");';
    html += 'var values = Array.from(checkboxes).map(function(cb) { return cb.value; });';
    html += 'var filterKey = type === "fy" ? "fiscalYears" : type === "year" ? "years" : type === "month" ? "months" : type === "commodity" ? "commodities" : type === "department" ? "departments" : type + "s";';
    html += 'state.filters[filterKey] = values;';
    html += 'var display = document.getElementById(type + "Display");';
    html += 'var displayEl = document.querySelector("#" + type + "MultiSelect .multi-select-display");';
    html += 'if (values.length === 0) { display.textContent = defaultLabel; if (displayEl) displayEl.classList.remove("active"); }';
    html += 'else if (values.length === 1) { display.textContent = values[0]; if (displayEl) displayEl.classList.add("active"); }';
    html += 'else { display.textContent = values.length + " selected"; if (displayEl) displayEl.classList.add("active"); }';
    // When FY or Year changes, filter the months dropdown
    html += 'if (type === "fy" || type === "year") { filterMonthsDropdown(); }';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    html += 'function selectAllFilter(type) {';
    html += 'document.querySelectorAll("#" + type + "Options input[type=checkbox]").forEach(function(cb) { if (!cb.closest(".multi-select-option").classList.contains("hidden")) cb.checked = true; });';
    html += 'var defaultLabel = type === "fy" ? "All FY" : type === "year" ? "All Years" : type === "month" ? "All Months" : type === "commodity" ? "All Commodities" : type === "department" ? "All Departments" : "All " + type.charAt(0).toUpperCase() + type.slice(1) + "s";';
    html += 'updateMultiFilter(type, defaultLabel); }';

    html += 'function clearAllFilter(type) {';
    html += 'document.querySelectorAll("#" + type + "Options input[type=checkbox]").forEach(function(cb) { cb.checked = false; });';
    html += 'var defaultLabel = type === "fy" ? "All FY" : type === "year" ? "All Years" : type === "month" ? "All Months" : type === "commodity" ? "All Commodities" : type === "department" ? "All Departments" : "All " + type.charAt(0).toUpperCase() + type.slice(1) + "s";';
    html += 'updateMultiFilter(type, defaultLabel); }';

    // Keep old customer functions for backward compatibility
    html += 'function updateCustomerFilter() { updateMultiFilter("customer", "All Customers"); }';
    html += 'function selectAllCustomers() { selectAllFilter("customer"); }';
    html += 'function clearAllCustomers() { clearAllFilter("customer"); }';

    // Export to Excel
    html += 'function exportToExcel() {';
    html += 'var items = state.data.items || [];';
    html += 'var csv = "Style,Style Name,Commodity,Customer,SO#,Delivery Date,Qty,Amount\\n";';
    html += 'items.forEach(function(item) {';
    html += 'item.orders.forEach(function(o) {';
    html += 'var row = [';
    html += '"\\"" + (item.style_number || "").replace(/"/g, \'"\') + "\\""';
    html += ',"\\"" + (item.style_name || "").replace(/"/g, \'"\') + "\\""';
    html += ',"\\"" + (item.commodity || "").replace(/"/g, \'"\') + "\\""';
    html += ',"\\"" + (o.customer || "").replace(/"/g, \'"\') + "\\""';
    html += ',"\\"" + (o.so_number || "").replace(/"/g, \'"\') + "\\""';
    html += ',o.delivery_date || ""';
    html += ',o.quantity || 0';
    html += ',o.total_amount || 0';
    html += '].join(",");';
    html += 'csv += row + "\\n";';
    html += '}); });';
    html += 'var blob = new Blob([csv], { type: "text/csv" });';
    html += 'var url = URL.createObjectURL(blob);';
    html += 'var a = document.createElement("a"); a.href = url; a.download = "open_orders_export.csv"; a.click(); }';

    // Toggle SO expansion
    html += 'function toggleSO(idx) { var el = document.getElementById("so-items-" + idx); el.classList.toggle("expanded"); }';

    // Toggle month collapse/expand
    html += 'function toggleMonth(monthKey) {';
    html += 'var section = document.getElementById("month-" + monthKey);';
    html += 'if (!section) return;';
    html += 'var header = section.querySelector(".month-header");';
    html += 'var body = document.getElementById("month-body-" + monthKey);';
    html += 'if (header) header.classList.toggle("collapsed");';
    html += 'if (body) body.classList.toggle("collapsed"); }';

    // Toggle commodity collapse/expand
    html += 'function toggleCommodity(commId) {';
    html += 'var body = document.getElementById("comm-body-" + commId);';
    html += 'if (!body) return;';
    html += 'var section = body.closest(".commodity-section");';
    html += 'var header = section ? section.querySelector(".commodity-header") : null;';
    html += 'if (header) header.classList.toggle("collapsed");';
    html += 'if (body) body.classList.toggle("collapsed"); }';

    // Scroll to month from timeline
    html += 'function scrollToMonth(monthKey) {';
    html += 'var el = document.getElementById("month-" + monthKey);';
    html += 'if (el) el.scrollIntoView({ behavior: "smooth", block: "start" });';
    html += 'document.querySelectorAll(".timeline-month").forEach(function(m) { m.classList.remove("active"); }); }';

    // Show style detail
    html += 'async function showStyleDetail(styleNumber) {';
    html += 'var item = state.data.items.find(function(i) { return i.style_number === styleNumber; });';
    html += 'if (!item) return;';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc.includes("workdrive.zoho.com") || imgSrc.includes("download-accl.zoho.com")) {';
    html += 'var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'document.getElementById("modalImage").src = imgSrc;';
    html += 'document.getElementById("modalStyleName").textContent = item.style_name || item.style_number;';
    html += 'document.getElementById("modalStyleNumber").textContent = item.style_number + (item.commodity ? " · " + item.commodity : "");';
    html += 'document.getElementById("modalQty").textContent = formatNumber(item.total_qty);';
    html += 'document.getElementById("modalDollars").textContent = formatNumber(Math.round(item.total_dollars));';
    html += 'document.getElementById("modalOrders").textContent = item.order_count;';
    html += 'var ordersHtml = "";';
    html += 'item.orders.forEach(function(o) {';
    html += 'var date = o.delivery_date ? new Date(o.delivery_date).toLocaleDateString("en-US", {month: "short", day: "numeric"}) : "TBD";';
    html += 'ordersHtml += \'<div class="order-row"><div class="order-row-left"><div class="order-row-so"><a href="#" class="zoho-doc-link" data-type="salesorder" data-num="\' + escapeHtml(o.so_number) + \'" style="text-decoration:underline;color:inherit;cursor:pointer" title="Open in Zoho Inventory">SO# \' + escapeHtml(o.so_number) + \'</a></div><div class="order-row-customer">\' + escapeHtml(o.customer) + (o.color ? " · " + escapeHtml(o.color) : "") + \'</div></div>\';';
    html += 'ordersHtml += \'<div class="order-row-right"><div class="order-row-qty">\' + formatNumber(o.quantity) + \' units</div><div class="order-row-date">\' + date + \'</div></div></div>\'; });';
    html += 'document.getElementById("modalOrdersList").innerHTML = ordersHtml;';
    html += 'document.getElementById("styleModal").classList.add("active"); }';

    // Close modal
    html += 'function closeModal() { document.getElementById("styleModal").classList.remove("active"); }';

    // Show PO Style Detail modal (for Import POs)
    html += 'async function showPOStyleDetail(styleNumber) {';
    html += 'var item = state.data.items.find(function(i) { return i.style_number === styleNumber; });';
    html += 'if (!item) return;';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc.includes("workdrive.zoho.com") || imgSrc.includes("download-accl.zoho.com")) {';
    html += 'var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'document.getElementById("modalImage").src = imgSrc;';
    html += 'document.getElementById("modalStyleName").textContent = item.style_name || item.style_number;';
    html += 'document.getElementById("modalStyleNumber").textContent = item.style_number + (item.commodity ? " · " + item.commodity : "");';
    html += 'document.getElementById("modalQty").textContent = formatNumber(item.total_qty);';
    html += 'document.getElementById("modalDollars").textContent = formatNumber(Math.round(item.total_dollars));';
    // Dedupe POs by po_number - consolidate duplicates
    html += 'var poMap = {};';
    html += 'if (item.pos && item.pos.length > 0) {';
    html += 'item.pos.forEach(function(po) {';
    html += 'var key = (po.po_number || "") + "|" + (po.color || "");';
    html += 'if (!poMap[key]) { poMap[key] = { po_number: po.po_number, vendor_name: po.vendor_name, color: po.color, po_warehouse_date: po.po_warehouse_date, po_quantity: 0, po_total: 0, po_unit_price: parseFloat(po.po_unit_price) || 0 }; }';
    html += 'poMap[key].po_quantity += parseFloat(po.po_quantity) || 0;';
    html += 'poMap[key].po_total += parseFloat(po.po_total) || 0;';
    html += '}); }';
    html += 'var uniquePOs = Object.values(poMap);';
    html += 'var poCount = uniquePOs.length;';
    html += 'document.getElementById("modalOrders").textContent = poCount;';
    // Check for price variance across POs
    html += 'var prices = uniquePOs.map(function(po) { return po.po_unit_price; }).filter(function(p) { return p > 0; });';
    html += 'var minPrice = prices.length > 0 ? Math.min.apply(null, prices) : 0;';
    html += 'var maxPrice = prices.length > 0 ? Math.max.apply(null, prices) : 0;';
    html += 'var hasPriceVariance = minPrice > 0 && maxPrice > 0 && (maxPrice - minPrice) > 0.01;';
    // Update the label to say "POs" instead of "Orders"
    html += 'var ordersLabel = document.querySelector("#modalOrders").parentElement.querySelector(".modal-stat-label");';
    html += 'if (ordersLabel) ordersLabel.textContent = "POs";';
    html += 'var listTitle = document.querySelector("#modalOrdersList").parentElement.querySelector("h4");';
    html += 'if (listTitle) listTitle.textContent = "Purchase Orders";';
    // Build PO list from deduplicated data
    html += 'var posHtml = "";';
    // Show price variance warning if different prices exist
    html += 'if (hasPriceVariance) {';
    html += 'posHtml += \'<div style="background:#fff3cd;border:1px solid #ffc107;border-radius:6px;padding:0.5rem 0.75rem;margin-bottom:0.75rem;font-size:0.8rem">\';';
    html += 'posHtml += \'<span style="color:#856404">⚠️ <strong>Price Variance:</strong> $\' + minPrice.toFixed(2) + \' - $\' + maxPrice.toFixed(2) + \'/unit</span>\';';
    html += 'posHtml += \'</div>\'; }';
    html += 'if (uniquePOs.length > 0) {';
    html += 'uniquePOs.forEach(function(po) {';
    html += 'var date = po.po_warehouse_date ? new Date(po.po_warehouse_date).toLocaleDateString("en-US", {month: "short", day: "numeric"}) : "TBD";';
    html += 'var unitPrice = po.po_unit_price ? "$" + po.po_unit_price.toFixed(2) : "";';
    html += 'var isHighPrice = hasPriceVariance && po.po_unit_price === maxPrice;';
    html += 'var isLowPrice = hasPriceVariance && po.po_unit_price === minPrice;';
    html += 'var priceStyle = isHighPrice ? "color:#dc3545;font-weight:700" : (isLowPrice ? "color:#28a745;font-weight:700" : "color:#0088c2;font-weight:600");';
    html += 'var priceIndicator = isHighPrice ? " ▲" : (isLowPrice ? " ▼" : "");';
    html += 'posHtml += \'<div class="order-row"><div class="order-row-left"><div class="order-row-so"><a href="#" class="zoho-doc-link" data-type="purchaseorder" data-num="\' + escapeHtml(po.po_number || "-") + \'" style="text-decoration:underline;color:inherit;cursor:pointer" title="Open in Zoho Inventory">PO# \' + escapeHtml(po.po_number || "-") + \'</a>\' + (unitPrice ? \' <span style="\' + priceStyle + \'">\' + unitPrice + \'/u\' + priceIndicator + \'</span>\' : "") + \'</div><div class="order-row-customer">\' + escapeHtml(po.vendor_name || "Unknown Vendor") + (po.color ? " · " + escapeHtml(po.color) : "") + \'</div></div>\';';
    html += 'posHtml += \'<div class="order-row-right"><div class="order-row-qty">\' + formatNumber(po.po_quantity || 0) + \' units</div><div class="order-row-date">\' + date + \'</div></div></div>\'; });';
    html += '} else { posHtml = \'<div style="color:#86868b;font-size:0.875rem;padding:0.5rem 0">No PO details available</div>\'; }';
    html += 'document.getElementById("modalOrdersList").innerHTML = posHtml;';
    html += 'document.getElementById("styleModal").classList.add("active"); }';

    // Upload modal
    html += 'function showUploadModal() { document.getElementById("uploadModal").classList.add("active"); }';
    html += 'function closeUploadModal() { document.getElementById("uploadModal").classList.remove("active"); document.getElementById("uploadProgress").classList.remove("active"); }';

    // Settings modal
    html += 'function showSettingsModal() {';
    html += 'document.getElementById("settingsModal").classList.add("active");';
    html += 'var select = document.getElementById("defaultFYSelect");';
    html += 'select.innerHTML = \'<option value="">No Default (show all)</option>\';';
    // Populate default FY dropdown from fyOptions checkboxes
    html += 'var fyOptions = document.querySelectorAll("#fyOptions input[type=checkbox]");';
    html += 'fyOptions.forEach(function(cb) { select.innerHTML += \'<option value="\' + cb.value + \'">FY\' + cb.value + \'</option>\'; });';
    // Populate FY checkboxes from API (all fiscal years, not just filtered)
    html += 'var fyCheckboxes = document.getElementById("fyCheckboxes");';
    html += 'fyCheckboxes.innerHTML = "<span style=\\"color:#86868b;font-size:0.75rem\\">Loading...</span>";';
    html += 'fetch("/api/filters").then(r => r.json()).then(function(filterData) {';
    html += 'var allFYs = filterData.allFiscalYears || filterData.fiscalYears || [];';
    html += 'fyCheckboxes.innerHTML = "";';
    html += 'allFYs.forEach(function(fy) {';
    html += 'fyCheckboxes.innerHTML += \'<label style="display:flex;align-items:center;gap:0.25rem;padding:0.5rem 0.75rem;background:#f0f4f8;border-radius:6px;cursor:pointer;font-size:0.875rem"><input type="checkbox" class="fy-include-cb" value="\' + fy + \'" checked> FY\' + fy + \'</label>\';';
    html += '});';
    // Now load settings to uncheck excluded FYs
    html += 'fetch("/api/settings").then(r => r.json()).then(function(settings) {';
    html += 'console.log("Loaded settings for FY checkboxes:", settings);';
    html += 'if (settings.includeFiscalYears) {';
    html += 'var included = settings.includeFiscalYears.split(",");';
    html += 'console.log("Included FYs from settings:", included);';
    html += 'document.querySelectorAll(".fy-include-cb").forEach(function(cb) { console.log("CB", cb.value, "in", included, "?", included.includes(cb.value)); cb.checked = included.includes(cb.value); });';
    html += '} else { console.log("No includeFiscalYears setting found"); }';
    html += '});';
    html += '});';
    html += 'fetch("/api/settings").then(r => r.json()).then(function(settings) {';
    html += 'if (settings.defaultFiscalYear) select.value = settings.defaultFiscalYear;';
    html += '});';
    html += 'updateStatusBadges();';
    html += 'checkWorkDriveStatus();';
    html += 'checkImportPOStatus();';
    html += '}';

    // Toggle advanced settings
    html += 'function toggleAdvancedSettings() {';
    html += 'var el = document.getElementById("advancedSettings");';
    html += 'var arrow = document.getElementById("advancedArrow");';
    html += 'if (el.style.display === "none") { el.style.display = "block"; arrow.textContent = "▼"; }';
    html += 'else { el.style.display = "none"; arrow.textContent = "▶"; }';
    html += '}';

    // Status badges for the compact status section
    html += 'async function updateStatusBadges() {';
    html += 'try {';
    html += 'var res = await fetch("/api/zoho/status");';
    html += 'var data = await res.json();';
    html += 'var tokenBadge = document.getElementById("tokenStatusBadge");';
    html += 'var imagesBadge = document.getElementById("imagesCachedBadge");';
    html += 'if (!data.configured) { tokenBadge.innerHTML = \'<span style="color:#f59e0b">⚠️ Not configured</span>\'; }';
    html += 'else if (data.connected && !data.lastTokenError) { tokenBadge.innerHTML = \'<span style="color:#16a34a">✓ Active</span>\'; }';
    html += 'else if (data.lastTokenError) { tokenBadge.innerHTML = \'<span style="color:#ef4444">✗ \' + data.lastTokenError + "</span>"; }';
    html += 'else { tokenBadge.innerHTML = \'<span style="color:#f59e0b">Not initialized</span>\'; }';
    html += 'if (data.cache) { imagesBadge.textContent = data.cache.cachedImages.toLocaleString(); }';
    html += '} catch(e) { document.getElementById("tokenStatusBadge").innerHTML = \'<span style="color:#ef4444">Error</span>\'; }';
    html += 'try {';
    html += 'var res2 = await fetch("/api/workdrive/status");';
    html += 'var wd = await res2.json();';
    html += 'var wdBadge = document.getElementById("workdriveStatusBadge");';
    html += 'var syncBadge = document.getElementById("lastSyncBadge");';
    html += 'if (wd.configured) { wdBadge.innerHTML = \'<span style="color:#16a34a">✓ Connected</span>\'; }';
    html += 'else { wdBadge.innerHTML = \'<span style="color:#f59e0b">⚠️ Not configured</span>\'; }';
    html += 'if (wd.lastSync) { syncBadge.textContent = new Date(wd.lastSync).toLocaleString() + " (" + (wd.lastSyncRecords || 0) + " records)"; }';
    html += 'else { syncBadge.innerHTML = \'<span style="color:#f59e0b">Never</span>\'; }';
    html += '} catch(e) { document.getElementById("workdriveStatusBadge").innerHTML = \'<span style="color:#ef4444">Error</span>\'; }';
    html += '}';

    // Trigger Export & Sync - the main action button
    html += 'async function triggerExportAndSync() {';
    html += 'var btn = document.getElementById("triggerExportBtn");';
    html += 'var result = document.getElementById("triggerExportResult");';
    html += 'btn.disabled = true;';
    html += 'btn.textContent = "⏳ Triggering export...";';
    html += 'result.innerHTML = \'<span style="color:#007aff">Step 1: Calling Zoho Flow to export data...</span>\';';
    html += 'try {';
    html += 'var res = await fetch("/api/trigger-export", { method: "POST" });';
    html += 'var data = await res.json();';
    html += 'if (!data.success) {';
    html += 'result.innerHTML = \'<span style="color:#ef4444">Export failed: \' + (data.error || "Unknown error") + "</span>";';
    html += 'btn.disabled = false; btn.textContent = "📤 Trigger Export & Sync"; return;';
    html += '}';
    html += 'result.innerHTML = \'<span style="color:#007aff">Step 2: Export triggered! Waiting for file to land in WorkDrive...</span>\';';
    html += 'btn.textContent = "⏳ Waiting for file...";';
    // Poll for the new file, then sync
    html += 'var attempts = 0; var maxAttempts = 30;';
    html += 'var pollInterval = setInterval(async function() {';
    html += 'attempts++;';
    html += 'if (attempts > maxAttempts) {';
    html += 'clearInterval(pollInterval);';
    html += 'result.innerHTML = \'<span style="color:#f59e0b">⚠️ Export triggered but file hasn\\\'t appeared yet. Try clicking Sync Sales Orders in a few minutes.</span>\';';
    html += 'btn.disabled = false; btn.textContent = "📤 Trigger Export & Sync"; return;';
    html += '}';
    html += 'result.innerHTML = \'<span style="color:#007aff">Step 2: Waiting for file... (\' + (attempts * 10) + "s)</span>";';
    html += '}, 10000);';
    // Wait 30s then try syncing
    html += 'setTimeout(async function() {';
    html += 'clearInterval(pollInterval);';
    html += 'btn.textContent = "⏳ Syncing...";';
    html += 'result.innerHTML = \'<span style="color:#007aff">Step 3: Importing data from WorkDrive...</span>\';';
    html += 'try {';
    html += 'var syncRes = await fetch("/api/workdrive/sync?force=true", { method: "POST" });';
    html += 'var syncData = await syncRes.json();';
    html += 'if (syncData.success && !syncData.skipped) {';
    html += 'result.innerHTML = \'<span style="color:#16a34a">✓ Done! Imported \' + syncData.imported + " records from " + syncData.fileName + "</span>";';
    html += 'updateStatusBadges();';
    html += 'setTimeout(function() { location.reload(); }, 2000);';
    html += '} else if (syncData.skipped) {';
    html += 'result.innerHTML = \'<span style="color:#f59e0b">⚠️ \' + syncData.message + ". New file may still be processing — try again in a minute.</span>";';
    html += 'btn.disabled = false; btn.textContent = "📤 Trigger Export & Sync";';
    html += '} else {';
    html += 'result.innerHTML = \'<span style="color:#ef4444">Sync error: \' + syncData.error + "</span>";';
    html += 'btn.disabled = false; btn.textContent = "📤 Trigger Export & Sync";';
    html += '}';
    html += '} catch(se) {';
    html += 'result.innerHTML = \'<span style="color:#ef4444">Sync failed: \' + se.message + "</span>";';
    html += 'btn.disabled = false; btn.textContent = "📤 Trigger Export & Sync";';
    html += '}';
    html += '}, 30000);';
    html += '} catch(e) {';
    html += 'result.innerHTML = \'<span style="color:#ef4444">Error: \' + e.message + "</span>";';
    html += 'btn.disabled = false; btn.textContent = "📤 Trigger Export & Sync";';
    html += '}';
    html += '}';

    html += 'function closeSettingsModal() { document.getElementById("settingsModal").classList.remove("active"); document.getElementById("settingsStatus").textContent = ""; }';
    html += 'async function saveSettings() {';
    html += 'var fy = document.getElementById("defaultFYSelect").value;';
    html += 'var includedFYs = Array.from(document.querySelectorAll(".fy-include-cb:checked")).map(function(cb) { return cb.value; }).join(",");';
    html += 'console.log("Saving settings - defaultFY:", fy, "includedFYs:", includedFYs);';
    html += 'try {';
    html += 'var r1 = await fetch("/api/settings", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: "defaultFiscalYear", value: fy }) });';
    html += 'var d1 = await r1.json(); console.log("Save defaultFY result:", d1);';
    html += 'var r2 = await fetch("/api/settings", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: "includeFiscalYears", value: includedFYs }) });';
    html += 'var d2 = await r2.json(); console.log("Save includeFYs result:", d2);';
    html += 'document.getElementById("settingsStatus").innerHTML = \'<span style="color:#34c759">✓ Settings saved! Reloading...</span>\';';
    html += 'setTimeout(function() { location.reload(); }, 1000);';
    html += '} catch(e) { console.error("Save error:", e); document.getElementById("settingsStatus").innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + \'</span>\'; }';
    html += '}';

    // Management PIN functions
    html += 'function showPinModal() { document.getElementById("pinModal").classList.add("active"); document.getElementById("pinInput").value = ""; document.getElementById("pinError").textContent = ""; setTimeout(function() { document.getElementById("pinInput").focus(); }, 100); }';
    html += 'function closePinModal() { document.getElementById("pinModal").classList.remove("active"); }';
    html += 'async function verifyPin() {';
    html += 'var pin = document.getElementById("pinInput").value;';
    html += 'if (!pin) { document.getElementById("pinError").textContent = "Please enter a PIN"; return; }';
    html += 'try {';
    html += 'var res = await fetch("/api/verify-pin", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ pin: pin }) });';
    html += 'var data = await res.json();';
    html += 'if (data.valid) { localStorage.setItem("mgmtPinUnlocked", "true"); showTotalValue(); closePinModal(); }';
    html += 'else { document.getElementById("pinError").textContent = "Incorrect PIN"; document.getElementById("pinInput").value = ""; }';
    html += '} catch(e) { document.getElementById("pinError").textContent = "Error: " + e.message; }';
    html += '}';
    html += 'function showTotalValue() { document.getElementById("statDollarsBox").style.display = ""; document.getElementById("statDollarsLock").style.display = "none"; }';
    html += 'function hideTotalValue() { document.getElementById("statDollarsBox").style.display = "none"; document.getElementById("statDollarsLock").style.display = ""; }';
    html += 'function relockTotalValue() { localStorage.removeItem("mgmtPinUnlocked"); hideTotalValue(); }';
    html += 'async function checkPinOnLoad() {';
    html += 'try {';
    html += 'var res = await fetch("/api/pin-status");';
    html += 'var data = await res.json();';
    html += 'if (!data.hasPin) { showTotalValue(); return; }';
    html += 'if (localStorage.getItem("mgmtPinUnlocked") === "true") { showTotalValue(); }';
    html += 'else { hideTotalValue(); }';
    html += '} catch(e) { console.error("PIN check error:", e); hideTotalValue(); }';
    html += '}';
    html += 'async function saveMgmtPin() {';
    html += 'var pin = document.getElementById("mgmtPinInput").value;';
    html += 'if (!pin) { document.getElementById("mgmtPinStatus").innerHTML = \'<span style="color:#ff3b30">Enter a PIN first</span>\'; return; }';
    html += 'try {';
    html += 'var res = await fetch("/api/settings", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: "managementPin", value: pin }) });';
    html += 'var data = await res.json();';
    html += 'if (data.success) { document.getElementById("mgmtPinStatus").innerHTML = \'<span style="color:#34c759">✓ PIN saved!</span>\'; document.getElementById("mgmtPinInput").value = ""; localStorage.setItem("mgmtPinUnlocked", "true"); }';
    html += '} catch(e) { document.getElementById("mgmtPinStatus").innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + \'</span>\'; }';
    html += '}';
    html += 'async function clearMgmtPin() {';
    html += 'try {';
    html += 'var res = await fetch("/api/settings", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ key: "managementPin", value: "" }) });';
    html += 'var data = await res.json();';
    html += 'if (data.success) { document.getElementById("mgmtPinStatus").innerHTML = \'<span style="color:#34c759">✓ PIN removed. Total value visible to all.</span>\'; showTotalValue(); }';
    html += '} catch(e) { document.getElementById("mgmtPinStatus").innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + \'</span>\'; }';
    html += '}';

    // Pre-cache functions
    html += 'var preCacheInterval = null;';
    html += 'async function startPreCache() {';
    html += 'var btn = document.getElementById("preCacheBtn");';
    html += 'var status = document.getElementById("preCacheStatus");';
    html += 'btn.disabled = true;';
    html += 'btn.textContent = "⏳ Starting...";';
    html += 'try {';
    html += 'var res = await fetch("/api/images/precache", { method: "POST" });';
    html += 'var data = await res.json();';
    html += 'if (data.status === "already_running") { status.textContent = "Already running..."; }';
    html += 'else { status.textContent = "Started!"; }';
    html += 'preCacheInterval = setInterval(checkPreCacheProgress, 2000);';
    html += 'checkPreCacheProgress();';
    html += '} catch(e) { status.textContent = "Error: " + e.message; btn.disabled = false; btn.textContent = "🖼️ Pre-Cache Images"; }';
    html += '}';
    html += 'async function checkPreCacheProgress() {';
    html += 'var btn = document.getElementById("preCacheBtn");';
    html += 'var status = document.getElementById("preCacheStatus");';
    html += 'try {';
    html += 'var res = await fetch("/api/images/precache/status");';
    html += 'var data = await res.json();';
    html += 'if (data.running) {';
    html += 'btn.textContent = "⏳ Caching...";';
    html += 'var phase = data.progress.phase || "working";';
    html += 'if (phase === "refreshing token") { status.textContent = "Refreshing Zoho token..."; }';
    html += 'else if (phase === "discovering") { status.textContent = "Finding uncached images..."; }';
    html += 'else if (phase.startsWith("token error")) { status.innerHTML = \'<span style="color:#ff3b30">\' + phase + \'</span>\'; }';
    html += 'else if (data.progress.total > 0) { status.textContent = data.progress.done + "/" + data.progress.total + " images (" + data.progress.errors + " errors)"; }';
    html += 'else { status.textContent = "Starting..."; }';
    html += '} else {';
    html += 'clearInterval(preCacheInterval);';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "🖼️ Pre-Cache Images";';
    html += 'var phase = data.progress.phase || "";';
    html += 'if (phase.startsWith("stopped") || phase.startsWith("token error") || phase.startsWith("error")) {';
    html += 'status.innerHTML = \'<span style="color:#ff3b30">⚠️ \' + phase + " (" + data.progress.done + " cached)</span>";';
    html += '} else if (data.progress.total > 0) { status.innerHTML = \'<span style="color:#34c759">✓ Done! \' + data.progress.done + " cached</span>"; }';
    html += 'else { status.textContent = "All images cached!"; }';
    html += '}';
    html += '} catch(e) { clearInterval(preCacheInterval); }';
    html += '}';

    // Zoho token status functions
    html += 'async function checkTokenStatus() {';
    html += 'var statusEl = document.getElementById("tokenStatusText");';
    html += 'if (!statusEl) return;';
    html += 'try {';
    html += 'var res = await fetch("/api/zoho/status");';
    html += 'var data = await res.json();';
    html += 'if (!data.configured) {';
    html += 'statusEl.innerHTML = \'<span style="color:#ff9500">⚠️ Zoho credentials not configured</span>\';';
    html += '} else if (data.connected && !data.lastTokenError) {';
    html += 'statusEl.innerHTML = \'<span style="color:#34c759">✓ Token active</span> · \' + data.cache.cachedImages + " images cached";';
    html += '} else if (data.lastTokenError) {';
    html += 'statusEl.innerHTML = \'<span style="color:#ff3b30">⚠️ Token error: \' + data.lastTokenError + \'</span>\';';
    html += '} else {';
    html += 'statusEl.innerHTML = \'<span style="color:#ff9500">Token not initialized</span>\';';
    html += '}';
    html += '} catch(e) { statusEl.innerHTML = \'<span style="color:#ff3b30">Error checking status</span>\'; }';
    html += '}';
    html += 'async function refreshZohoToken() {';
    html += 'var btn = document.getElementById("refreshTokenBtn");';
    html += 'var status = document.getElementById("refreshTokenStatus");';
    html += 'btn.disabled = true;';
    html += 'btn.textContent = "⏳ Refreshing...";';
    html += 'try {';
    html += 'var res = await fetch("/api/zoho/refresh-token", { method: "POST" });';
    html += 'var data = await res.json();';
    html += 'if (data.success) {';
    html += 'status.innerHTML = \'<span style="color:#34c759">✓ Token refreshed!</span>\';';
    html += 'checkTokenStatus();';
    html += '} else {';
    html += 'status.innerHTML = \'<span style="color:#ff3b30">Failed: \' + data.error + \'</span>\';';
    html += '}';
    html += '} catch(e) { status.innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + \'</span>\'; }';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "🔄 Refresh Token";';
    html += '}';

    // WorkDrive folder sync functions
    html += 'async function checkWorkDriveStatus() {';
    html += 'var statusEl = document.getElementById("workdriveStatusText");';
    html += 'if (!statusEl) return;';
    html += 'statusEl.innerHTML = "Checking WorkDrive folder...";';
    html += 'try {';
    html += 'var res = await fetch("/api/workdrive/status");';
    html += 'var data = await res.json();';
    html += 'if (data.error) {';
    html += 'statusEl.innerHTML = \'<span style="color:#ff3b30">Error: \' + data.error + "</span>";';
    html += 'return;';
    html += '}';
    html += 'if (!data.configured) {';
    html += 'statusEl.innerHTML = \'<span style="color:#ff9500">⚠️ Not configured</span><br><span style="font-size:0.75rem;color:#86868b">Set WORKDRIVE_SYNC_FOLDER_ID in environment</span>\';';
    html += 'document.getElementById("workdriveSyncBtn").disabled = true;';
    html += 'return;';
    html += '}';
    html += 'var html = \'<span style="color:#34c759">✓ Folder connected</span><br>\';';
    html += 'if (data.latestAvailableFile) {';
    html += 'html += \'<span style="font-size:0.75rem;color:#1e3a5f">Latest file: \' + data.latestAvailableFile + "</span><br>";';
    html += '}';
    html += 'if (data.lastSync) {';
    html += 'var syncDate = new Date(data.lastSync).toLocaleString();';
    html += 'html += \'<span style="font-size:0.75rem;color:#86868b">Last sync: \' + syncDate + " (" + (data.lastSyncRecords || 0) + " records)</span><br>";';
    html += 'if (data.lastSyncFile) {';
    html += 'html += \'<span style="font-size:0.75rem;color:#86868b">File: \' + data.lastSyncFile + "</span>";';
    html += '}';
    html += '} else {';
    html += 'html += \'<span style="font-size:0.75rem;color:#ff9500">Never synced</span>\';';
    html += '}';
    html += 'statusEl.innerHTML = html;';
    html += '} catch(e) { statusEl.innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + "</span>"; }';
    html += '}';
    html += 'async function syncFromWorkDrive() {';
    html += 'var btn = document.getElementById("workdriveSyncBtn");';
    html += 'var result = document.getElementById("workdriveSyncResult");';
    html += 'btn.disabled = true;';
    html += 'btn.textContent = "⏳ Syncing...";';
    html += 'result.innerHTML = \'<span style="color:#007aff">Fetching latest CSV from WorkDrive...</span>\';';
    html += 'try {';
    html += 'var res = await fetch("/api/workdrive/sync?force=true", { method: "POST" });';
    html += 'var data = await res.json();';
    html += 'if (data.success) {';
    html += 'if (data.skipped) {';
    html += 'result.innerHTML = \'<span style="color:#ff9500">⚠️ \' + data.message + "</span>";';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "🔄 Sync from WorkDrive";';
    html += '} else {';
    html += 'result.innerHTML = \'<span style="color:#34c759">✓ Imported \' + data.imported + " records from " + data.fileName + "</span>";';
    html += 'setTimeout(function() { location.reload(); }, 1500);';
    html += '}';
    html += '} else {';
    html += 'result.innerHTML = \'<span style="color:#ff3b30">Error: \' + data.error + "</span>";';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "🔄 Sync from WorkDrive";';
    html += '}';
    html += '} catch(e) {';
    html += 'result.innerHTML = \'<span style="color:#ff3b30">Sync failed: \' + e.message + "</span>";';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "🔄 Sync from WorkDrive";';
    html += '}';
    html += 'checkWorkDriveStatus();';
    html += '}';

    // Import PO sync functions
    html += 'async function checkImportPOStatus() {';
    html += 'var statusEl = document.getElementById("importPOStatus");';
    html += 'if (!statusEl) return;';
    html += 'try {';
    html += 'var res = await fetch("/api/workdrive/import-po/status");';
    html += 'var data = await res.json();';
    html += 'if (data.error) {';
    html += 'statusEl.innerHTML = \'<span style="color:#ff3b30">Error: \' + data.error + "</span>";';
    html += 'return;';
    html += '}';
    html += 'if (!data.configured) {';
    html += 'statusEl.innerHTML = \'<span style="color:#ff9500">⚠️ Not configured</span><br><span style="font-size:0.75rem;color:#86868b">Set WORKDRIVE_SYNC_FOLDER_ID in Railway environment</span>\';';
    html += 'document.getElementById("importPOSyncBtn").disabled = true;';
    html += 'return;';
    html += '}';
    html += 'var html = \'<span style="color:#34c759">✓ Import PO folder connected</span><br>\';';
    html += 'if (data.latestAvailableFile) {';
    html += 'html += \'<span style="font-size:0.75rem;color:#1e3a5f">Latest file: \' + data.latestAvailableFile + "</span><br>";';
    html += '}';
    html += 'if (data.lastSync) {';
    html += 'var syncDate = new Date(data.lastSync).toLocaleString();';
    html += 'html += \'<span style="font-size:0.75rem;color:#86868b">Last sync: \' + syncDate + " (" + (data.lastSyncRecords || 0) + " records)</span><br>";';
    html += '} else {';
    html += 'html += \'<span style="font-size:0.75rem;color:#ff9500">Never synced</span>\';';
    html += '}';
    html += 'statusEl.innerHTML = html;';
    html += '} catch(e) { statusEl.innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + "</span>"; }';
    html += '}';
    html += 'async function syncImportPOs() {';
    html += 'var btn = document.getElementById("importPOSyncBtn");';
    html += 'var result = document.getElementById("importPOSyncResult");';
    html += 'btn.disabled = true;';
    html += 'btn.textContent = "⏳ Syncing...";';
    html += 'result.innerHTML = \'<span style="color:#007aff">Fetching Import PO CSV from WorkDrive...</span>\';';
    html += 'try {';
    html += 'var res = await fetch("/api/workdrive/import-po/sync?force=true", { method: "POST" });';
    html += 'var data = await res.json();';
    html += 'if (data.success) {';
    html += 'if (data.skipped) {';
    html += 'result.innerHTML = \'<span style="color:#ff9500">⚠️ \' + data.message + "</span>";';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "📦 Sync Import SOs & POs";';
    html += '} else {';
    html += 'var msg = "✓ " + (data.imported || 0) + " inserted, " + (data.updated || 0) + " updated from " + data.fileName;';
    html += 'result.innerHTML = \'<span style="color:#34c759">\' + msg + "</span>";';
    html += 'setTimeout(function() { location.reload(); }, 1500);';
    html += '}';
    html += '} else {';
    html += 'result.innerHTML = \'<span style="color:#ff3b30">Error: \' + data.error + "</span>";';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "📦 Sync Import SOs & POs";';
    html += '}';
    html += '} catch(e) {';
    html += 'result.innerHTML = \'<span style="color:#ff3b30">Sync failed: \' + e.message + "</span>";';
    html += 'btn.disabled = false;';
    html += 'btn.textContent = "📦 Sync Import SOs & POs";';
    html += '}';
    html += 'checkImportPOStatus();';
    html += '}';

    // WorkDrive folder browser
    html += 'var folderHistory = [];';
    html += 'function toggleFolderBrowser() {';
    html += 'var browser = document.getElementById("workdriveFolderBrowser");';
    html += 'if (browser.style.display === "none") {';
    html += 'browser.style.display = "block";';
    html += 'folderHistory = [];';
    html += 'browseFolder(null);';
    html += '} else {';
    html += 'browser.style.display = "none";';
    html += '}';
    html += '}';
    html += 'async function browseFolder(folderId) {';
    html += 'var list = document.getElementById("folderList");';
    html += 'var breadcrumb = document.getElementById("folderBreadcrumb");';
    html += 'list.innerHTML = \'<span style="color:#86868b">Loading...</span>\';';
    html += 'try {';
    html += 'var url = "/api/workdrive/browse" + (folderId ? "?folderId=" + folderId : "");';
    html += 'var res = await fetch(url);';
    html += 'var data = await res.json();';
    html += 'if (!data.success) {';
    html += 'list.innerHTML = \'<span style="color:#ff3b30">\' + data.error + "</span>";';
    html += 'return;';
    html += '}';
    html += 'var items = data.folders || data.items || [];';
    html += 'if (folderId) {';
    html += 'if (folderHistory.length === 0 || folderHistory[folderHistory.length - 1].id !== folderId) {';
    html += 'folderHistory.push({ id: folderId, name: "Folder" });';
    html += '}';
    html += '}';
    // Build breadcrumb
    html += 'breadcrumb.innerHTML = \'<a href="#" onclick="browseFolder(null); folderHistory=[]; return false;" style="color:#007aff">🏠 Root</a>\';';
    html += 'folderHistory.forEach(function(f, i) {';
    html += 'breadcrumb.innerHTML += \' / <a href="#" onclick="goToFolder(\' + i + \'); return false;" style="color:#007aff">\' + f.name + "</a>";';
    html += '});';
    // Build folder list
    html += 'list.innerHTML = "";';
    html += 'if (items.length === 0) {';
    html += 'list.innerHTML = \'<span style="color:#86868b;font-size:0.75rem">No items found</span>\';';
    html += 'return;';
    html += '}';
    html += 'items.forEach(function(item) {';
    html += 'var div = document.createElement("div");';
    html += 'div.style.cssText = "display:flex;align-items:center;gap:0.5rem;padding:0.375rem;cursor:pointer;border-radius:4px;font-size:0.8125rem";';
    html += 'div.onmouseover = function() { this.style.background = "#f0f4f8"; };';
    html += 'div.onmouseout = function() { this.style.background = "transparent"; };';
    html += 'if (item.type === "folder" || item.type === "team_folder") {';
    html += 'div.innerHTML = \'📁 <span style="flex:1">\' + item.name + \'</span><button onclick="selectFolder(event, \\"\' + item.id + \'\\", \\"\' + item.name.replace(/"/g, \'\') + \'\\")" style="padding:0.25rem 0.5rem;font-size:0.6875rem;background:#007aff;color:#fff;border:none;border-radius:4px;cursor:pointer">Use This</button>\';';
    html += 'div.onclick = function(e) { if (e.target.tagName !== "BUTTON") { folderHistory.push({ id: item.id, name: item.name }); browseFolder(item.id); } };';
    html += '} else {';
    html += 'div.innerHTML = \'📄 <span style="flex:1;color:#86868b">\' + item.name + "</span>";';
    html += '}';
    html += 'list.appendChild(div);';
    html += '});';
    html += '} catch(e) { list.innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + "</span>"; }';
    html += '}';
    html += 'function goToFolder(index) {';
    html += 'folderHistory = folderHistory.slice(0, index + 1);';
    html += 'browseFolder(folderHistory[index].id);';
    html += '}';
    html += 'function selectFolder(event, folderId, folderName) {';
    html += 'event.stopPropagation();';
    html += 'var result = document.getElementById("workdriveSyncResult");';
    html += 'result.innerHTML = \'<span style="color:#007aff">Selected folder: \' + folderName + "</span><br>" +';
    html += '\'<span style="font-size:0.75rem;color:#1e3a5f">Folder ID: <code style="background:#f0f4f8;padding:0.125rem 0.25rem;border-radius:3px">\' + folderId + "</code></span><br>" +';
    html += '\'<span style="font-size:0.75rem;color:#86868b">Add this to Railway: <code>WORKDRIVE_SYNC_FOLDER_ID=\' + folderId + "</code></span>";';
    html += 'document.getElementById("workdriveFolderBrowser").style.display = "none";';
    html += '}';
    html += 'async function testFolderId() {';
    html += 'var folderId = document.getElementById("manualFolderId").value.trim();';
    html += 'var teamId = document.getElementById("manualTeamId").value.trim();';
    html += 'if (!folderId) { alert("Please enter a folder ID"); return; }';
    html += 'var list = document.getElementById("folderList");';
    html += 'list.innerHTML = \'<span style="color:#86868b">Testing connection...</span>\';';
    html += 'try {';
    html += 'var url = "/api/workdrive/browse?folderId=" + encodeURIComponent(folderId);';
    html += 'if (teamId) url += "&teamId=" + encodeURIComponent(teamId);';
    html += 'var res = await fetch(url);';
    html += 'var data = await res.json();';
    html += 'if (data.success) {';
    html += 'var csvCount = data.items.filter(function(i) { return i.name && i.name.toLowerCase().endsWith(".csv"); }).length;';
    html += 'list.innerHTML = \'<span style="color:#34c759">✓ Connected! Found \' + data.items.length + " items (" + csvCount + " CSV files)</span>";';
    html += 'var result = document.getElementById("workdriveSyncResult");';
    html += 'var envVars = "WORKDRIVE_SYNC_FOLDER_ID=" + folderId;';
    html += 'if (teamId) envVars += "\\nWORKDRIVE_TEAM_ID=" + teamId;';
    html += 'result.innerHTML = \'<span style="color:#34c759">✓ Connection successful!</span><br>\' +';
    html += '\'<span style="font-size:0.75rem;color:#1e3a5f">Add to Railway environment variables:</span><br>\' +';
    html += '\'<pre style="font-size:0.7rem;background:#f0f4f8;padding:0.5rem;border-radius:4px;margin-top:0.25rem;white-space:pre-wrap">\' + envVars + "</pre>";';
    html += '} else {';
    html += 'list.innerHTML = \'<span style="color:#ff3b30">✗ \' + (data.error || "Could not access folder") + "</span>";';
    html += '}';
    html += '} catch(e) { list.innerHTML = \'<span style="color:#ff3b30">Error: \' + e.message + "</span>"; }';
    html += '}';

    // File upload
    html += 'var uploadArea = document.getElementById("uploadArea");';
    html += 'var fileInput = document.getElementById("fileInput");';
    html += 'uploadArea.addEventListener("click", function() { fileInput.click(); });';
    html += 'uploadArea.addEventListener("dragover", function(e) { e.preventDefault(); uploadArea.classList.add("dragover"); });';
    html += 'uploadArea.addEventListener("dragleave", function() { uploadArea.classList.remove("dragover"); });';
    html += 'uploadArea.addEventListener("drop", function(e) { e.preventDefault(); uploadArea.classList.remove("dragover"); if (e.dataTransfer.files.length) handleFile(e.dataTransfer.files[0]); });';
    html += 'fileInput.addEventListener("change", function() { if (fileInput.files.length) handleFile(fileInput.files[0]); });';

    html += 'async function handleFile(file) {';
    html += 'if (!file.name.endsWith(".csv")) { alert("Please upload a CSV file"); return; }';
    html += 'document.getElementById("uploadProgress").classList.add("active");';
    html += 'document.getElementById("progressFill").style.width = "30%";';
    html += 'document.getElementById("uploadStatus").textContent = "Uploading...";';
    html += 'var formData = new FormData(); formData.append("file", file);';
    html += 'try { var res = await fetch("/api/import?clearMode=replace", { method: "POST", body: formData });';
    html += 'document.getElementById("progressFill").style.width = "100%";';
    html += 'var data = await res.json();';
    html += 'if (data.success) { document.getElementById("uploadStatus").textContent = "Success! Imported " + data.imported + " items."; setTimeout(function() { closeUploadModal(); loadFilters(); loadData(); }, 1500); }';
    html += 'else { document.getElementById("uploadStatus").textContent = "Error: " + data.error; }';
    html += '} catch(e) { document.getElementById("uploadStatus").textContent = "Error: " + e.message; }}';

    // Mode switch function
    html += 'function switchMode(mode) {';
    html += 'if (state.mode === mode) return;';
    html += 'state.mode = mode;';
    html += 'state.view = "dashboard";'; // Reset to dashboard when switching modes
    html += 'state.poTilesLimit = 200;'; // Reset tile limit when switching modes
    html += 'document.querySelectorAll(".mode-btn").forEach(function(b) { b.classList.remove("active"); });';
    html += 'document.querySelector(".mode-btn[data-mode=\'" + mode + "\']").classList.add("active");';
    html += 'if (mode === "po") {';
    html += 'document.getElementById("dashboardTitle").textContent = "Import POs";';
    html += 'document.getElementById("statOrders").parentElement.querySelector(".stat-label").textContent = "Open POs";';
    html += 'document.getElementById("statCustomers").parentElement.querySelector(".stat-label").textContent = "Vendors";';
    html += 'document.querySelectorAll(".status-btn")[0].textContent = "Open";';
    html += 'document.querySelectorAll(".status-btn")[1].textContent = "Received";';
    html += 'document.querySelectorAll(".status-btn")[1].dataset.status = "Received";';
    // Update Customer label to Vendor and load vendor data
    html += 'var custLabel = document.querySelector("#customerMultiSelect").closest(".filter-group").querySelector(".filter-label");';
    html += 'if (custLabel) custLabel.textContent = "Vendor";';
    html += 'document.getElementById("customerDisplay").textContent = "All Vendors";';
    html += 'state.filters.customers = [];';
    // Load vendor list from PO filters
    html += 'fetch("/api/po/filters?status=" + (state.filters.poStatus || "Open")).then(function(r) { return r.json(); }).then(function(pf) {';
    html += 'populateMultiSelect("customer", pf.vendors.map(function(v) { return { value: v.vendor_name, label: v.vendor_name + " (" + v.po_count + " POs)" }; }), "All Vendors");';
    html += 'populateMultiSelect("commodity", pf.commodities.map(function(c) { return { value: c.commodity, label: c.commodity + " (" + c.style_count + ")" }; }), "All Commodities");';
    html += '}).catch(function(e) { console.error("Error loading PO filters:", e); });';
    // Hide year/FY/month filters (not relevant for PO mode - uses warehouse date timeline)
    html += 'document.getElementById("yearMultiSelect") && (document.getElementById("yearMultiSelect").closest(".filter-group").style.display = "none");';
    html += 'document.getElementById("fyMultiSelect") && (document.getElementById("fyMultiSelect").closest(".filter-group").style.display = "none");';
    html += 'document.getElementById("monthMultiSelect") && (document.getElementById("monthMultiSelect").closest(".filter-group").style.display = "none");';
    // Update view toggle for PO mode
    html += 'document.querySelector(".view-toggle").innerHTML = \'<button class="view-btn active" data-view="dashboard">📊 Dashboard</button><button class="view-btn" data-view="styles">By Style</button><button class="view-btn" data-view="summary">Summary</button><button class="view-btn" data-view="orders">By Vendor</button><button class="view-btn" data-view="pricecompare">💰 Price Compare</button><button class="view-btn" data-view="charts">📈 Charts</button>\';';
    html += 'bindViewToggle();';
    html += '} else {';
    html += 'document.getElementById("dashboardTitle").textContent = "Open Orders";';
    html += 'document.getElementById("statOrders").parentElement.querySelector(".stat-label").textContent = "Open Orders";';
    html += 'document.getElementById("statCustomers").parentElement.querySelector(".stat-label").textContent = "Customers";';
    html += 'document.querySelectorAll(".status-btn")[0].textContent = "Open";';
    html += 'document.querySelectorAll(".status-btn")[1].textContent = "Invoiced";';
    html += 'document.querySelectorAll(".status-btn")[1].dataset.status = "Invoiced";';
    // Restore Customer label and reload customer data
    html += 'var custLabel = document.querySelector("#customerMultiSelect").closest(".filter-group").querySelector(".filter-label");';
    html += 'if (custLabel) custLabel.textContent = "Customer";';
    html += 'document.getElementById("customerDisplay").textContent = "All Customers";';
    html += 'state.filters.customers = [];';
    // Show year/FY/month filters again
    html += 'document.getElementById("yearMultiSelect") && (document.getElementById("yearMultiSelect").closest(".filter-group").style.display = "");';
    html += 'document.getElementById("fyMultiSelect") && (document.getElementById("fyMultiSelect").closest(".filter-group").style.display = "");';
    html += 'document.getElementById("monthMultiSelect") && (document.getElementById("monthMultiSelect").closest(".filter-group").style.display = "");';
    // Restore view toggle for Sales mode
    html += 'document.querySelector(".view-toggle").innerHTML = \'<button class="view-btn active" data-view="dashboard">📊 Dashboard</button><button class="view-btn" data-view="summary">Summary</button><button class="view-btn" data-view="styles">By Style</button><button class="view-btn" data-view="topmovers">🏆 Top Movers</button><button class="view-btn" data-view="opportunities">🎯 Opportunities</button><button class="view-btn" data-view="orders">By SO#</button><button class="view-btn" data-view="charts">Charts</button><button class="view-btn" data-view="merchandising">📈 Merchandising</button>\';';
    html += 'bindViewToggle();';
    html += 'loadFilters();';
    html += '}';
    html += 'loadData();';
    html += '}';
    // Function to rebind view toggle events
    html += 'function bindViewToggle() {';
    html += 'document.querySelectorAll(".view-btn").forEach(function(btn) { btn.addEventListener("click", function() {';
    html += 'document.querySelectorAll(".view-btn").forEach(function(b) { b.classList.remove("active"); });';
    html += 'btn.classList.add("active"); state.view = btn.dataset.view; loadData(); }); });';
    html += '}';

    // Filter handlers - multi-selects are handled by updateMultiFilter function
    // Sort by filter
    html += 'document.getElementById("sortByFilter").addEventListener("change", function(e) { state.sortBy = e.target.value; if (state.data) renderContent(state.data); });';

    // Status toggle
    html += 'document.querySelectorAll(".status-btn").forEach(function(btn) { btn.addEventListener("click", function() {';
    html += 'document.querySelectorAll(".status-btn").forEach(function(b) { b.classList.remove("active"); });';
    html += 'btn.classList.add("active"); if (state.mode === "po") { state.filters.poStatus = btn.dataset.status; } else { state.filters.status = btn.dataset.status; } loadData(); }); });';

    // View toggle
    html += 'document.querySelectorAll(".view-btn").forEach(function(btn) { btn.addEventListener("click", function() {';
    html += 'document.querySelectorAll(".view-btn").forEach(function(b) { b.classList.remove("active"); });';
    html += 'btn.classList.add("active"); state.view = btn.dataset.view; loadData(); }); });';

    // Style search handlers
    html += 'var styleSearchTimeout = null;';
    html += 'function handleStyleSearch(e) {';
    html += 'clearTimeout(styleSearchTimeout);';
    html += 'var val = e.target.value.trim();';
    html += 'document.getElementById("styleSearchClear").style.display = val ? "block" : "none";';
    html += 'styleSearchTimeout = setTimeout(function() {';
    html += 'state.styleSearch = val;';
    html += 'updateClearButton();';
    html += 'loadData();';
    html += '}, 300); }';
    html += 'function clearStyleSearch() {';
    html += 'document.getElementById("styleSearchInput").value = "";';
    html += 'document.getElementById("styleSearchClear").style.display = "none";';
    html += 'state.styleSearch = "";';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    // Clear filters
    html += 'function clearFilters() {';
    html += 'state.filters = { years: [], fiscalYears: [], customers: [], vendors: [], months: [], commodities: [], departments: [], status: state.filters.status, poStatus: state.filters.poStatus };';
    html += 'state.styleSearch = "";';
    html += 'state.poTilesLimit = 200;'; // Reset tile limit when clearing filters
    html += 'document.getElementById("styleSearchInput").value = "";';
    html += 'document.getElementById("styleSearchClear").style.display = "none";';
    // Clear all multi-select checkboxes and reset displays
    html += '["year", "fy", "customer", "month", "commodity", "department"].forEach(function(type) {';
    html += 'document.querySelectorAll("#" + type + "Dropdown input[type=checkbox]").forEach(function(cb) { cb.checked = false; });';
    html += 'var display = document.getElementById(type + "Display");';
    html += 'var displayEl = document.querySelector("#" + type + "MultiSelect .multi-select-display");';
    html += 'if (display) display.textContent = type === "fy" ? "All FY" : type === "year" ? "All Years" : type === "month" ? "All Months" : type === "commodity" ? "All Commodities" : type === "department" ? "All Departments" : "All Customers";';
    html += 'if (displayEl) displayEl.classList.remove("active");';
    html += '});';
    // Reset months dropdown to show all months
    html += 'filterMonthsDropdown();';
    html += 'updateClearButton(); loadData(); }';
    html += 'function updateClearButton() { var hasFilters = state.filters.customers.length > 0 || state.filters.months.length > 0 || state.filters.commodities.length > 0 || state.filters.departments.length > 0 || state.filters.years.length > 0 || state.filters.fiscalYears.length > 0 || state.styleSearch;';
    html += 'document.getElementById("clearFiltersBtn").style.display = hasFilters ? "block" : "none"; }';

    // Chat
    html += 'function toggleChat() { document.getElementById("chatPanel").classList.toggle("active"); }';
    html += 'document.getElementById("chatInput").addEventListener("keypress", function(e) { if (e.key === "Enter" && !e.shiftKey) { e.preventDefault(); sendChat(); }});';

    html += 'async function sendChat() {';
    html += 'var input = document.getElementById("chatInput");';
    html += 'var message = input.value.trim(); if (!message) return;';
    html += 'var messages = document.getElementById("chatMessages");';
    html += 'messages.innerHTML += \'<div class="chat-message user">\' + escapeHtml(message) + \'</div>\';';
    html += 'messages.innerHTML += \'<div class="chat-typing" id="typing"><span></span><span></span><span></span></div>\';';
    html += 'messages.scrollTop = messages.scrollHeight; input.value = "";';
    html += 'try { var res = await fetch("/api/chat", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ message: message }) });';
    html += 'var data = await res.json();';
    html += 'document.getElementById("typing").remove();';
    html += 'messages.innerHTML += \'<div class="chat-message assistant">\' + escapeHtml(data.response) + \'</div>\';';
    html += 'messages.scrollTop = messages.scrollHeight;';
    html += 'if (data.filters) { applyFiltersFromChat(data.filters); }';
    html += '} catch(e) { document.getElementById("typing").remove(); messages.innerHTML += \'<div class="chat-message assistant">Sorry, something went wrong.</div>\'; }}';

    html += 'function applyFiltersFromChat(filters) {';
    html += 'console.log("Applying chat filters:", filters);';
    // Apply all filters at once (not just one)
    html += 'if (filters.customer) {';
    html += 'document.querySelectorAll("#customerOptions input[type=checkbox]").forEach(function(cb) { cb.checked = cb.value === filters.customer; });';
    html += 'state.filters.customers = [filters.customer];';
    html += 'document.getElementById("customerDisplay").textContent = filters.customer;';
    html += 'var custDisplay = document.querySelector("#customerMultiSelect .multi-select-display"); if(custDisplay) custDisplay.classList.add("active");';
    html += '}';
    html += 'if (filters.commodity) {';
    html += 'document.querySelectorAll("#commodityOptions input[type=checkbox]").forEach(function(cb) { cb.checked = cb.value === filters.commodity; });';
    html += 'state.filters.commodities = [filters.commodity];';
    html += 'document.getElementById("commodityDisplay").textContent = filters.commodity;';
    html += 'var commDisplay = document.querySelector("#commodityMultiSelect .multi-select-display"); if(commDisplay) commDisplay.classList.add("active");';
    html += '}';
    html += 'if (filters.month) {';
    html += 'document.querySelectorAll("#monthOptions input[type=checkbox]").forEach(function(cb) { cb.checked = cb.value === filters.month; });';
    html += 'state.filters.months = [filters.month];';
    html += 'document.getElementById("monthDisplay").textContent = filters.month;';
    html += 'var monthDisplay = document.querySelector("#monthMultiSelect .multi-select-display"); if(monthDisplay) monthDisplay.classList.add("active");';
    html += '}';
    // Switch to dashboard view if not already there
    html += 'if (state.view !== "dashboard") {';
    html += 'state.view = "dashboard";';
    html += 'document.querySelectorAll(".view-btn").forEach(function(b) { b.classList.remove("active"); });';
    html += 'var dashBtn = document.querySelector(".view-btn[data-view=\'dashboard\']"); if(dashBtn) dashBtn.classList.add("active");';
    html += '}';
    html += 'updateClearButton(); loadData(); }';

    // Helpers
    html += 'function formatNumber(n) { return n.toString().replace(/\\B(?=(\\d{3})+(?!\\d))/g, ","); }';
    html += 'function formatMoney(n) { n = n || 0; if (n >= 1000000) { var m = (n / 1000000).toFixed(1); if (m.endsWith(".0")) m = m.slice(0,-2); return "$" + m + "M"; } return "$" + formatNumber(Math.round(n)); }';
    html += 'function calcGM(item) { var totalQty = 0, weightedMargin = 0; if (item.orders) { item.orders.forEach(function(o) { var sp = o.unit_price || 0; var cp = o.po_unit_price || 0; var qty = o.quantity || 0; if (sp > 0 && cp > 0 && qty > 0) { var margin = ((sp - cp) / sp) * 100; weightedMargin += margin * qty; totalQty += qty; } }); } if (totalQty <= 0) return { value: null, cls: "gm-none", label: "N/A" }; var gm = weightedMargin / totalQty; var cls = gm >= 40 ? "gm-high" : (gm >= 25 ? "gm-mid" : "gm-low"); return { value: gm, cls: cls, label: gm.toFixed(1) + "%" }; }';
    html += 'function escapeHtml(str) { if (!str) return ""; return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); }';
    html += 'document.addEventListener("click",function(e){var link=e.target.closest(".zoho-doc-link");if(!link)return;e.preventDefault();var type=link.dataset.type;var num=link.dataset.num;var origText=link.textContent;link.style.opacity="0.5";link.textContent=origText+" ⏳";fetch("/api/zoho-link/"+type+"/"+encodeURIComponent(num)).then(function(r){return r.json()}).then(function(d){link.style.opacity="1";link.textContent=origText;if(d.url)window.open(d.url,"zoho-doc")}).catch(function(){link.style.opacity="1";link.textContent=origText})});';
    html += 'function handleImgError(img, fileId) {';
    html += 'if (!fileId) { img.style.display = "none"; return; }';
    html += 'var btn = document.createElement("button");';
    html += 'btn.className = "img-retry-btn";';
    html += 'btn.innerHTML = "📷 Get Image";';
    html += 'btn.onclick = function(e) { e.stopPropagation(); retryImage(btn, fileId); };';
    html += 'img.replaceWith(btn);';
    html += '}';
    html += 'async function retryImage(btn, fileId) {';
    html += 'btn.classList.add("loading");';
    html += 'btn.innerHTML = "⏳ Loading...";';
    html += 'try {';
    html += 'var res = await fetch("/api/image/" + fileId);';
    html += 'if (res.ok) {';
    html += 'var blob = await res.blob();';
    html += 'var img = document.createElement("img");';
    html += 'img.src = URL.createObjectURL(blob);';
    html += 'img.style.maxWidth = "90%"; img.style.maxHeight = "90%"; img.style.objectFit = "contain";';
    html += 'btn.replaceWith(img);';
    html += '} else { btn.innerHTML = "❌ Failed"; btn.classList.remove("loading"); setTimeout(function() { btn.innerHTML = "📷 Retry"; }, 2000); }';
    html += '} catch(e) { btn.innerHTML = "❌ Error"; btn.classList.remove("loading"); setTimeout(function() { btn.innerHTML = "📷 Retry"; }, 2000); }';
    html += '}';
    html += 'var monthNames = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];';
    html += 'var monthNamesFull = ["January","February","March","April","May","June","July","August","September","October","November","December"];';
    html += 'function formatMonthLabel(ym) { var parts = ym.split("-"); return monthNamesFull[parseInt(parts[1])-1] + " " + parts[0]; }';
    html += 'function formatMonthShort(ym) { var parts = ym.split("-"); return monthNames[parseInt(parts[1])-1] + " " + parts[0].slice(2); }';

    // Modal click outside
    html += 'document.getElementById("styleModal").addEventListener("click", function(e) { if (e.target === this) closeModal(); });';
    html += 'document.getElementById("uploadModal").addEventListener("click", function(e) { if (e.target === this) closeUploadModal(); });';
    html += 'document.getElementById("settingsModal").addEventListener("click", function(e) { if (e.target === this) closeSettingsModal(); });';

    // Init - load settings first to apply default fiscal year
    html += '(async function() {';
    html += 'try { var settings = await fetch("/api/settings").then(r => r.json());';
    html += 'if (settings.defaultFiscalYear) { state.filters.fiscalYear = settings.defaultFiscalYear; }';
    html += '} catch(e) { console.log("No settings found, using defaults"); }';
    html += 'await loadFilters(); loadData(); checkPinOnLoad();';
    html += '})();';

    html += '</script></body></html>';
    return html;
}

// Start server
initDB().then(function() {
    app.listen(PORT, function() {
        console.log('Mark Edwards Open Orders Dashboard running on port ' + PORT);

        // Schedule automatic WorkDrive sync at 3 AM daily (server timezone)
        if (WORKDRIVE_SYNC_FOLDER_ID) {
            cron.schedule('0 3 * * *', async function() {
                console.log('=== SCHEDULED WORKDRIVE SYNC STARTING (3 AM) ===');
                try {
                    var result = await syncFromWorkDriveFolder(false); // Don't force - only sync if file changed
                    if (result.success) {
                        if (result.skipped) {
                            console.log('Scheduled sync: No new file to import');
                        } else {
                            console.log('Scheduled sync: Imported ' + result.imported + ' records from ' + result.fileName);
                        }
                    } else {
                        console.error('Scheduled sync failed:', result.error);
                    }
                } catch (err) {
                    console.error('Scheduled sync error:', err);
                }
                console.log('=== SCHEDULED WORKDRIVE SYNC COMPLETE ===');
            }, {
                timezone: 'America/New_York' // Eastern Time
            });
            console.log('WorkDrive auto-sync scheduled for 3 AM ET daily');
        } else {
            console.log('WorkDrive auto-sync not enabled (no folder ID configured)');
        }

        // Schedule automatic Import PO sync at 2 AM daily
        if (WORKDRIVE_IMPORT_PO_FOLDER_ID) {
            cron.schedule('0 2 * * *', async function() {
                console.log('=== SCHEDULED IMPORT PO SYNC STARTING (2 AM) ===');
                try {
                    var result = await syncImportPOsFromWorkDrive(false); // Don't force - only sync if file changed
                    if (result.success) {
                        if (result.skipped) {
                            console.log('Scheduled Import PO sync: No new file to import');
                        } else {
                            console.log('Scheduled Import PO sync: ' + result.imported + ' inserted, ' + result.updated + ' updated from ' + result.fileName);
                        }
                    } else {
                        console.error('Scheduled Import PO sync failed:', result.error);
                    }
                } catch (err) {
                    console.error('Scheduled Import PO sync error:', err);
                }
                console.log('=== SCHEDULED IMPORT PO SYNC COMPLETE ===');
            }, {
                timezone: 'America/New_York' // Eastern Time
            });
            console.log('Import PO auto-sync scheduled for 2 AM ET daily');
        } else {
            console.log('Import PO auto-sync not enabled (no folder ID configured)');
        }

        // Schedule automatic image pre-cache at 4 AM daily (after CSV sync)
        cron.schedule('0 4 * * *', async function() {
            console.log('=== SCHEDULED IMAGE PRE-CACHE STARTING (4 AM) ===');
            try {
                await preCacheImages();
                console.log('Scheduled pre-cache: Complete');
            } catch (err) {
                console.error('Scheduled pre-cache error:', err);
            }
            console.log('=== SCHEDULED IMAGE PRE-CACHE COMPLETE ===');
        }, {
            timezone: 'America/New_York' // Eastern Time
        });
        console.log('Image pre-cache scheduled for 4 AM ET daily');
    });
});
// Deploy trigger Wed Feb  4 20:10:44 UTC 2026
// Deploy trigger Sun Feb  8 18:53:58 UTC 2026
