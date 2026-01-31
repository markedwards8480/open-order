const express = require('express');
const { Pool } = require('pg');
const multer = require('multer');
const session = require('express-session');
require('dotenv').config();

// All data is stored server-side in PostgreSQL:
// - Order data in order_items and sales_orders tables
// - Images cached in image_cache table (fetched once from Zoho, then stored)
// - Zoho tokens in zoho_tokens table
// No local filesystem or browser caching is used for persistence

const app = express();
const PORT = process.env.PORT || 3001;

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
// ZOHO API RATE LIMITING & QUEUE SYSTEM
// Based on best practices to avoid hitting API limits
// ============================================

// Concurrency control - max 25 simultaneous Zoho requests for faster initial loading
// Once images are cached in Railway DB, this won't matter - they'll be served instantly
const MAX_CONCURRENT_ZOHO_REQUESTS = 25;
var activeZohoRequests = 0;
var zohoRequestQueue = [];

// Process the Zoho request queue
function processZohoQueue() {
    while (zohoRequestQueue.length > 0 && activeZohoRequests < MAX_CONCURRENT_ZOHO_REQUESTS) {
        var next = zohoRequestQueue.shift();
        activeZohoRequests++;
        next.execute()
            .then(next.resolve)
            .catch(next.reject)
            .finally(() => {
                activeZohoRequests--;
                processZohoQueue();
            });
    }
}

// Queue a Zoho API request with concurrency control
function queueZohoRequest(requestFn) {
    return new Promise((resolve, reject) => {
        zohoRequestQueue.push({
            execute: requestFn,
            resolve: resolve,
            reject: reject
        });
        processZohoQueue();
    });
}

// Exponential backoff retry for Zoho API calls
async function fetchWithBackoff(url, options, maxRetries = 4) {
    var lastError;
    for (var attempt = 0; attempt < maxRetries; attempt++) {
        try {
            var response = await fetch(url, options);

            // Handle rate limiting (429)
            if (response.status === 429) {
                var retryAfter = response.headers.get('Retry-After');
                var waitTime = retryAfter ? parseInt(retryAfter) * 1000 : Math.pow(2, attempt) * 1000;
                console.log('Zoho rate limit hit, waiting ' + (waitTime/1000) + 's before retry...');
                await sleep(waitTime);
                continue;
            }

            // Handle token expiration (401)
            if (response.status === 401 && attempt < maxRetries - 1) {
                console.log('Zoho token expired, refreshing...');
                await refreshZohoToken();
                options.headers['Authorization'] = 'Zoho-oauthtoken ' + zohoAccessToken;
                continue;
            }

            return response;
        } catch (err) {
            lastError = err;
            var waitTime = Math.pow(2, attempt) * 1000; // 1s, 2s, 4s, 8s
            console.log('Zoho request failed, retry ' + (attempt + 1) + '/' + maxRetries + ' in ' + (waitTime/1000) + 's:', err.message);
            await sleep(waitTime);
        }
    }
    throw lastError || new Error('Max retries exceeded');
}

function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// Fetch image from Zoho with queue and backoff
async function fetchZohoImage(fileId) {
    return queueZohoRequest(async () => {
        if (!zohoAccessToken) await refreshZohoToken();

        var imageUrl = 'https://workdrive.zoho.com/api/v1/download/' + fileId;
        var response = await fetchWithBackoff(imageUrl, {
            headers: { 'Authorization': 'Zoho-oauthtoken ' + zohoAccessToken }
        });

        if (!response.ok) {
            throw new Error('Image not found: ' + response.status);
        }

        var buffer = Buffer.from(await response.arrayBuffer());
        var contentType = response.headers.get('content-type') || 'image/jpeg';

        return { buffer, contentType };
    });
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
async function refreshZohoToken() {
    try {
        var clientId = process.env.ZOHO_CLIENT_ID;
        var clientSecret = process.env.ZOHO_CLIENT_SECRET;
        var refreshToken = process.env.ZOHO_REFRESH_TOKEN;
        if (!clientId || !clientSecret || !refreshToken) {
            return { success: false, error: 'Zoho credentials not configured' };
        }
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
            var expiresAt = new Date(Date.now() + (data.expires_in * 1000));
            await pool.query('INSERT INTO zoho_tokens (access_token, refresh_token, expires_at, updated_at) VALUES ($1, $2, $3, NOW())',
                [zohoAccessToken, refreshToken, expiresAt]);
            return { success: true };
        }
        return { success: false, error: data.error || 'Token refresh failed' };
    } catch (err) {
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
        var month = req.query.month; // Format: YYYY-MM
        var year = req.query.year; // Calendar year filter
        var fiscalYear = req.query.fiscalYear; // Fiscal year filter (June 1 - May 31)
        var status = req.query.status || 'Open';
        var style = req.query.style;
        var soNumber = req.query.so_number;

        var conditions = [];
        var params = [];
        var paramIndex = 1;

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
            var customerList = customers.split(',').map(c => c.trim()).filter(c => c);
            if (customerList.length > 0) {
                conditions.push('customer = ANY($' + paramIndex++ + ')');
                params.push(customerList);
            }
        }
        if (commodity) {
            conditions.push('commodity = $' + paramIndex++);
            params.push(commodity);
        }
        if (year) {
            conditions.push("EXTRACT(YEAR FROM delivery_date) = $" + paramIndex++);
            params.push(parseInt(year));
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
        if (month) {
            conditions.push("TO_CHAR(delivery_date, 'YYYY-MM') = $" + paramIndex++);
            params.push(month);
        }
        if (style) {
            conditions.push('style_number = $' + paramIndex++);
            params.push(style);
        }
        if (soNumber) {
            conditions.push('so_number = $' + paramIndex++);
            params.push(soNumber);
        }

        var whereClause = conditions.length > 0 ? 'WHERE ' + conditions.join(' AND ') : '';

        // Debug logging
        console.log('=== /api/orders DEBUG ===');
        console.log('Where clause:', whereClause);
        console.log('Params:', params);

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
                    'status', status
                ) ORDER BY delivery_date, so_number) as orders,
                SUM(quantity) as total_qty,
                SUM(total_amount) as total_dollars,
                COUNT(DISTINCT so_number) as order_count
            FROM order_items
            ${whereClause}
            GROUP BY style_number, style_name, commodity, image_url
            ORDER BY style_number
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

        res.json({
            items: result.rows,
            stats: statsResult.rows[0]
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
        var month = req.query.month;
        var year = req.query.year;
        var fiscalYear = req.query.fiscalYear;
        var status = req.query.status || 'Open';

        var conditions = [];
        var params = [];
        var paramIndex = 1;

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
            var customerList = customers.split(',').map(c => c.trim()).filter(c => c);
            if (customerList.length > 0) {
                conditions.push('customer = ANY($' + paramIndex++ + ')');
                params.push(customerList);
            }
        }
        if (commodity) {
            conditions.push('commodity = $' + paramIndex++);
            params.push(commodity);
        }
        if (year) {
            conditions.push("EXTRACT(YEAR FROM delivery_date) = $" + paramIndex++);
            params.push(parseInt(year));
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
        if (month) {
            conditions.push("TO_CHAR(delivery_date, 'YYYY-MM') = $" + paramIndex++);
            params.push(month);
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

        res.json({
            customers: customersResult.rows,
            commodities: commoditiesResult.rows,
            months: monthsResult.rows,
            years: yearsResult.rows.map(r => r.year),
            fiscalYears: fiscalYearsResult.rows.map(r => r.fiscal_year)
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
            image_url: findColumn(headers, ['style_image', 'image_url', 'image', 'image_link', 'workdrive_link', 'picture', 'photo_url'])
        };

        console.log('Column mapping:', colMap);

        // Validate required columns
        if (colMap.so_number === -1) return res.status(400).json({ error: 'Missing required column: SO Number' });
        if (colMap.customer === -1) return res.status(400).json({ error: 'Missing required column: Customer' });
        if (colMap.style_number === -1) return res.status(400).json({ error: 'Missing required column: Style Number' });

        // Clear existing data (full refresh)
        var clearMode = req.body.clearMode || req.query.clearMode || 'replace';
        if (clearMode === 'replace') {
            await pool.query('DELETE FROM order_items');
            await pool.query('DELETE FROM sales_orders');
            console.log('Cleared existing order data');
        }

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

                // Normalize status - Open, Partial, Invoiced
                var statusLower = status.toLowerCase();
                if (statusLower.includes('partial')) {
                    status = 'Partial';
                } else if (statusLower.includes('invoice') || statusLower.includes('shipped') || statusLower.includes('complete') || statusLower.includes('billed')) {
                    status = 'Invoiced';
                } else {
                    status = 'Open';
                }

                await pool.query(`
                    INSERT INTO order_items (so_number, customer, style_number, style_name, commodity, color, quantity, unit_price, total_amount, delivery_date, status, image_url, updated_at)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, NOW())
                `, [soNumber, customer, styleNumber, styleName, commodity, color, quantity, unitPrice, totalAmount, deliveryDate, status, imageUrl]);

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

        res.json({
            success: true,
            imported: imported,
            errors: errors.slice(0, 10),
            message: `Imported ${imported} line items`
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
    for (var alias of aliases) {
        var idx = headers.indexOf(alias);
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

        var systemPrompt = `You are a helpful assistant for a sales order dashboard. You help sales reps find information about open orders.

Available filters:
- Customers: ${customers.join(', ')}
- Commodities: ${commodities.join(', ')}
- Delivery Months: ${months.join(', ')}

When the user asks to see orders, extract the relevant filters and return them in a JSON block.
Format your response as helpful text followed by a JSON block with filters:

Example response:
"Here are Burlington's denim orders for June 2025:
\`\`\`json
{"customer": "Burlington", "commodity": "Denim", "month": "2025-06"}
\`\`\`"

If no specific filters mentioned, explain what data is available.
Match customer/commodity names flexibly (case-insensitive, partial matches OK).
For months, accept formats like "June", "Jun 2025", "June 2025" and convert to YYYY-MM.`;

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
app.get('/api/image/:fileId', async function(req, res) {
    try {
        var fileId = req.params.fileId;

        // Check database cache first (no Zoho API call needed)
        var cacheResult = await pool.query(
            'SELECT image_data, content_type FROM image_cache WHERE file_id = $1',
            [fileId]
        );

        if (cacheResult.rows.length > 0 && cacheResult.rows[0].image_data) {
            // Return cached image from database - zero Zoho API calls
            console.log('✓ Cache HIT:', fileId);
            res.set('Content-Type', cacheResult.rows[0].content_type || 'image/jpeg');
            res.set('Cache-Control', 'public, max-age=31536000');
            return res.send(cacheResult.rows[0].image_data);
        }

        // Cache miss - need to fetch from Zoho
        console.log('○ Cache MISS, fetching from Zoho:', fileId);

        // Fetch from Zoho using queue system with rate limiting and backoff
        // This ensures we never exceed concurrency limits
        var result = await fetchZohoImage(fileId);

        // Store in database for persistent caching (future requests = zero Zoho calls)
        try {
            await pool.query(
                'INSERT INTO image_cache (file_id, image_data, content_type) VALUES ($1, $2, $3) ON CONFLICT (file_id) DO UPDATE SET image_data = $2, content_type = $3',
                [fileId, result.buffer, result.contentType]
            );
            console.log('✓ Cached in Railway DB:', fileId, '(' + (result.buffer.length / 1024).toFixed(1) + ' KB)');
        } catch (cacheErr) {
            console.log('✗ Cache FAILED:', fileId, cacheErr.message);
        }

        res.set('Content-Type', result.contentType);
        res.set('Cache-Control', 'public, max-age=31536000');
        res.send(result.buffer);

    } catch (err) {
        console.error('Image fetch error:', err.message);
        res.status(500).send('Error loading image');
    }
});

// Zoho status and cache stats
app.get('/api/zoho/status', async function(req, res) {
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
        queue: {
            activeRequests: activeZohoRequests,
            queuedRequests: zohoRequestQueue.length,
            maxConcurrent: MAX_CONCURRENT_ZOHO_REQUESTS
        },
        cache: {
            cachedImages: cachedCount,
            cacheSizeMB: parseFloat(cacheSizeMB),
            stylesWithImageUrls: parseInt(stylesWithImages.rows[0].count),
            stylesCached: parseInt(stylesCached.rows[0].count)
        }
    });
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

// ============================================
// MAIN HTML PAGE
// ============================================
app.get('/', function(req, res) {
    res.send(getHTML());
});

function getHTML() {
    var html = '<!DOCTYPE html><html lang="en"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1.0"><title>Open Orders Dashboard</title>';
    html += '<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>';
    html += '<style>';

    // Base styles - matching product catalog
    html += '*{margin:0;padding:0;box-sizing:border-box}body{font-family:-apple-system,BlinkMacSystemFont,"SF Pro Display","SF Pro Text",sans-serif;background:#f5f5f7;color:#1e3a5f;font-weight:400;-webkit-font-smoothing:antialiased}';

    // Header
    html += '.header{background:white;padding:0 2rem;height:56px;border-bottom:1px solid rgba(0,0,0,0.06);display:flex;justify-content:space-between;align-items:center;position:sticky;top:0;z-index:100}.header h1{font-size:1.125rem;font-weight:600;color:#1e3a5f;letter-spacing:-0.01em;display:flex;align-items:center;gap:0.5rem}.header h1 span{color:#0088c2}';
    html += '.header-right{display:flex;gap:1rem;align-items:center}';

    // Buttons
    html += '.btn{padding:0.625rem 1.25rem;border:none;border-radius:980px;cursor:pointer;font-size:0.875rem;font-weight:500;transition:all 0.2s;letter-spacing:-0.01em}.btn-primary{background:#1e3a5f;color:white}.btn-primary:hover{background:#2a4a6f}.btn-secondary{background:transparent;color:#0088c2;padding:0.5rem 1rem}.btn-secondary:hover{background:rgba(0,136,194,0.08)}.btn-success{background:#0088c2;color:white}.btn-success:hover{background:#007ab8}';

    // Stats bar
    html += '.stats-bar{background:white;padding:1.5rem 2rem;border-bottom:1px solid rgba(0,0,0,0.06);display:flex;gap:3rem;flex-wrap:wrap;align-items:center}';
    html += '.stat-box{}.stat-value{font-size:1.75rem;font-weight:600;color:#1e3a5f;letter-spacing:-0.02em}.stat-value.money::before{content:"$"}.stat-label{color:#86868b;font-size:0.75rem;font-weight:500;text-transform:uppercase;letter-spacing:0.02em;margin-top:0.125rem}';
    html += '.stat-box.highlight .stat-value{color:#0088c2}';

    // Filters bar
    html += '.filters-bar{background:white;padding:1rem 2rem;border-bottom:1px solid rgba(0,0,0,0.06);display:flex;gap:0.75rem;flex-wrap:wrap;align-items:center}';
    html += '.filter-group{display:flex;align-items:center;gap:0.5rem}.filter-label{font-size:0.8125rem;color:#86868b;font-weight:500}';
    html += '.filter-select{padding:0.5rem 2rem 0.5rem 1rem;border:1px solid #d2d2d7;border-radius:8px;font-size:0.875rem;background:white;color:#1e3a5f;cursor:pointer;appearance:none;background-image:url("data:image/svg+xml,%3Csvg xmlns=\'http://www.w3.org/2000/svg\' width=\'12\' height=\'12\' viewBox=\'0 0 12 12\'%3E%3Cpath fill=\'%2386868b\' d=\'M6 8L2 4h8z\'/%3E%3C/svg%3E");background-repeat:no-repeat;background-position:right 0.75rem center}.filter-select:focus{outline:none;border-color:#0088c2}';
    html += '.filter-select.active{border-color:#0088c2;background-color:rgba(0,136,194,0.05)}';
    html += '.status-toggle{display:flex;background:#f0f4f8;border-radius:980px;padding:3px}.status-btn{padding:0.5rem 1.25rem;border:none;background:transparent;cursor:pointer;font-size:0.8125rem;font-weight:500;border-radius:980px;transition:all 0.2s;color:#6e6e73}';
    html += '.status-btn.active{color:white}.status-btn[data-status="Open"].active{background:#34c759}.status-btn[data-status="Invoiced"].active{background:#8e8e93}.status-btn[data-status="All"].active{background:#0088c2}';
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
    html += '.multi-select{position:relative;min-width:180px}.multi-select-display{background:white;border:1px solid #d2d2d7;border-radius:8px;padding:0.5rem 0.75rem;cursor:pointer;display:flex;justify-content:space-between;align-items:center;font-size:0.875rem}.multi-select-display:hover{border-color:#0088c2}';
    html += '.multi-select-arrow{font-size:0.625rem;color:#86868b;margin-left:0.5rem}';
    html += '.multi-select-dropdown{position:absolute;top:100%;left:0;right:0;background:white;border:1px solid #d2d2d7;border-radius:8px;box-shadow:0 4px 12px rgba(0,0,0,0.15);max-height:300px;overflow-y:auto;display:none;z-index:100}';
    html += '.multi-select-dropdown.open{display:block}';
    html += '.multi-select-option{padding:0.5rem 0.75rem;cursor:pointer;display:flex;align-items:center;gap:0.5rem;font-size:0.875rem}.multi-select-option:hover{background:#f5f5f7}';
    html += '.multi-select-option input{margin:0}';
    html += '.multi-select-actions{padding:0.5rem;border-top:1px solid #e0e0e0;display:flex;gap:0.5rem}.multi-select-actions button{flex:1;padding:0.375rem;font-size:0.75rem;border:none;border-radius:4px;cursor:pointer}.multi-select-actions .select-all{background:#e8f4fc;color:#0088c2}.multi-select-actions .clear-all{background:#f5f5f7;color:#86868b}';

    // Charts
    html += '.charts-container{display:grid;grid-template-columns:repeat(auto-fit,minmax(400px,1fr));gap:1.5rem}';
    html += '.chart-card{background:white;border-radius:16px;padding:1.5rem;border:1px solid rgba(0,0,0,0.04)}';
    html += '.chart-card h3{font-size:1rem;font-weight:600;color:#1e3a5f;margin:0 0 1rem 0}';
    html += '.chart-wrapper{height:300px;position:relative}';
    html += '.export-btn{background:#0088c2;color:white;border:none;padding:0.625rem 1.25rem;border-radius:8px;font-size:0.875rem;font-weight:500;cursor:pointer;display:flex;align-items:center;gap:0.5rem;margin-bottom:1rem}.export-btn:hover{background:#006699}';

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

    // Style card
    html += '.style-card{background:white;border-radius:16px;overflow:hidden;cursor:pointer;transition:all 0.3s ease;border:1px solid rgba(0,0,0,0.04)}.style-card:hover{transform:translateY(-4px);box-shadow:0 12px 40px rgba(0,0,0,0.12)}';
    html += '.style-image{height:200px;background:#f5f5f7;display:flex;align-items:center;justify-content:center;overflow:hidden;position:relative}.style-image img{max-width:90%;max-height:90%;object-fit:contain}';
    html += '.style-badge{position:absolute;top:12px;right:12px;background:#0088c2;color:white;padding:0.25rem 0.75rem;border-radius:980px;font-size:0.6875rem;font-weight:600}';
    html += '.commodity-badge{position:absolute;top:12px;left:12px;background:rgba(30,58,95,0.9);color:white;padding:0.25rem 0.625rem;border-radius:6px;font-size:0.625rem;font-weight:500;text-transform:uppercase;letter-spacing:0.02em}';
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

    html += '</style></head><body>';

    // Header
    html += '<header class="header">';
    html += '<h1><span>Open Orders</span> Dashboard</h1>';
    html += '<div class="header-right">';
    html += '<button class="btn btn-secondary" onclick="showUploadModal()">Import CSV</button>';
    html += '<a href="' + (process.env.PRODUCT_CATALOG_URL || '#') + '" class="btn btn-secondary" target="_blank">Product Catalog</a>';
    html += '</div></header>';

    // Stats bar
    html += '<div class="stats-bar">';
    html += '<div class="stat-box"><div class="stat-value" id="statOrders">-</div><div class="stat-label">Open Orders</div></div>';
    html += '<div class="stat-box"><div class="stat-value" id="statCustomers">-</div><div class="stat-label">Customers</div></div>';
    html += '<div class="stat-box"><div class="stat-value" id="statStyles">-</div><div class="stat-label">Styles</div></div>';
    html += '<div class="stat-box"><div class="stat-value" id="statUnits">-</div><div class="stat-label">Units</div></div>';
    html += '<div class="stat-box highlight"><div class="stat-value money" id="statDollars">-</div><div class="stat-label">Total Value</div></div>';
    html += '</div>';

    // Filters bar
    html += '<div class="filters-bar">';
    html += '<div class="filter-group"><label class="filter-label">Cal Year</label><select class="filter-select" id="yearFilter"></select></div>';
    html += '<div class="filter-group"><label class="filter-label">Fiscal Year</label><select class="filter-select" id="fiscalYearFilter"></select></div>';
    html += '<div class="filter-group"><label class="filter-label">Customer</label><div class="multi-select" id="customerMultiSelect"><div class="multi-select-display" onclick="toggleMultiSelect(\'customer\')"><span id="customerDisplay">All Customers</span><span class="multi-select-arrow">▼</span></div><div class="multi-select-dropdown" id="customerDropdown"></div></div></div>';
    html += '<div class="filter-group"><label class="filter-label">Delivery</label><select class="filter-select" id="monthFilter"><option value="">All Months</option></select></div>';
    html += '<div class="filter-group"><label class="filter-label">Commodity</label><select class="filter-select" id="commodityFilter"><option value="">All Commodities</option></select></div>';
    html += '<div class="status-toggle"><button class="status-btn active" data-status="Open">Open</button><button class="status-btn" data-status="Invoiced">Invoiced</button><button class="status-btn" data-status="All">All</button></div>';
    html += '<button class="clear-filters" onclick="clearFilters()" style="display:none" id="clearFiltersBtn">Clear Filters</button>';
    html += '<div class="view-toggle"><button class="view-btn active" data-view="monthly">By Month</button><button class="view-btn" data-view="summary">Summary</button><button class="view-btn" data-view="styles">By Style</button><button class="view-btn" data-view="orders">By SO#</button><button class="view-btn" data-view="charts">Charts</button></div>';
    html += '</div>';

    // Main content
    html += '<main class="main"><div id="content"><div class="loading"><div class="spinner"></div></div></div></main>';

    // Style detail modal
    html += '<div class="modal" id="styleModal"><div class="modal-content"><button class="modal-close" onclick="closeModal()">&times;</button><div class="modal-body"><div class="modal-image"><img id="modalImage" src="" alt=""></div><div class="modal-details"><div class="modal-header"><h2 id="modalStyleName">-</h2><p id="modalStyleNumber">-</p></div><div class="modal-stats"><div class="modal-stat"><div class="modal-stat-value" id="modalQty">-</div><div class="modal-stat-label">Total Units</div></div><div class="modal-stat"><div class="modal-stat-value money" id="modalDollars">-</div><div class="modal-stat-label">Total Value</div></div><div class="modal-stat"><div class="modal-stat-value" id="modalOrders">-</div><div class="modal-stat-label">Orders</div></div></div><div class="orders-list"><h4>Orders</h4><div id="modalOrdersList"></div></div></div></div></div></div>';

    // Upload modal
    html += '<div class="modal upload-modal" id="uploadModal"><div class="modal-content"><button class="modal-close" onclick="closeUploadModal()">&times;</button><h2>Import Sales Orders</h2><p style="color:#86868b;margin-top:0.5rem">Upload a CSV file with your sales order data</p><div class="upload-area" id="uploadArea"><input type="file" id="fileInput" accept=".csv"><div class="upload-icon">📄</div><div class="upload-text">Drag & drop your CSV here or <strong>browse</strong></div></div><div class="upload-progress" id="uploadProgress"><div class="progress-bar"><div class="progress-fill" id="progressFill"></div></div><p style="margin-top:0.5rem;font-size:0.875rem;color:#86868b" id="uploadStatus">Uploading...</p></div></div></div>';

    // Chat bubble
    html += '<div class="chat-bubble" onclick="toggleChat()"><svg viewBox="0 0 24 24"><path d="M20 2H4c-1.1 0-2 .9-2 2v18l4-4h14c1.1 0 2-.9 2-2V4c0-1.1-.9-2-2-2z"/></svg><span class="chat-bubble-label">Ask AI</span></div>';

    // Chat panel
    html += '<div class="chat-panel" id="chatPanel"><div class="chat-header"><h3>AI Assistant</h3><button class="chat-close" onclick="toggleChat()">&times;</button></div><div class="chat-messages" id="chatMessages"><div class="chat-message assistant">Hi! I can help you find orders. Try asking things like:<br><br>• "Show me Burlington\'s orders for June"<br>• "What denim orders are shipping next month?"<br>• "Find Ross orders"</div></div><div class="chat-input-area"><textarea class="chat-input" id="chatInput" placeholder="Ask about orders..." rows="1"></textarea><button class="chat-send" onclick="sendChat()">Send</button></div></div>';

    // JavaScript
    html += '<script>';

    // State
    html += 'var state = { filters: { year: "", fiscalYear: "", customers: [], month: "", commodity: "", status: "Open" }, view: "monthly", data: null };';

    // Load filters
    html += 'async function loadFilters() {';
    html += 'try { var res = await fetch("/api/filters"); var data = await res.json();';
    // Year filter
    html += 'var yearSelect = document.getElementById("yearFilter");';
    html += 'var currentYear = new Date().getFullYear();';
    html += 'data.years = data.years || [];';
    html += 'if (data.years.length === 0) { for(var y = currentYear; y >= currentYear - 3; y--) data.years.push(y); }';
    html += 'var allOpt = document.createElement("option"); allOpt.value = ""; allOpt.textContent = "All Years"; allOpt.selected = true; yearSelect.appendChild(allOpt);';
    html += 'data.years.forEach(function(y) { var opt = document.createElement("option"); opt.value = y; opt.textContent = y; yearSelect.appendChild(opt); });';
    // Fiscal Year filter (June 1 - May 31)
    html += 'var fySelect = document.getElementById("fiscalYearFilter");';
    html += 'var fyAllOpt = document.createElement("option"); fyAllOpt.value = ""; fyAllOpt.textContent = "All FY"; fyAllOpt.selected = true; fySelect.appendChild(fyAllOpt);';
    html += 'data.fiscalYears = data.fiscalYears || [];';
    html += 'if (data.fiscalYears.length === 0) { for(var fy = currentYear + 1; fy >= currentYear - 3; fy--) data.fiscalYears.push(fy); }';
    html += 'data.fiscalYears.forEach(function(fy) { var opt = document.createElement("option"); opt.value = fy; opt.textContent = "FY" + fy; fySelect.appendChild(opt); });';
    // Customer multi-select
    html += 'var custDropdown = document.getElementById("customerDropdown");';
    html += 'custDropdown.innerHTML = \'<div class="multi-select-actions"><button class="select-all" onclick="selectAllCustomers()">Select All</button><button class="clear-all" onclick="clearAllCustomers()">Clear All</button></div>\';';
    html += 'data.customers.forEach(function(c) { var div = document.createElement("div"); div.className = "multi-select-option"; div.innerHTML = \'<input type="checkbox" value="\' + c.customer + \'" onchange="updateCustomerFilter()"><span>\' + c.customer + \' (\' + c.order_count + \')</span>\'; custDropdown.appendChild(div); });';
    // Month filter
    html += 'var monthSelect = document.getElementById("monthFilter");';
    html += 'data.months.forEach(function(m) { var opt = document.createElement("option"); opt.value = m.month; opt.textContent = m.display_name; monthSelect.appendChild(opt); });';
    // Commodity filter
    html += 'var commSelect = document.getElementById("commodityFilter");';
    html += 'data.commodities.forEach(function(c) { var opt = document.createElement("option"); opt.value = c.commodity; opt.textContent = c.commodity + " (" + c.style_count + ")"; commSelect.appendChild(opt); });';
    html += '} catch(e) { console.error("Error loading filters:", e); }}';

    // Load data
    html += 'async function loadData() {';
    html += 'document.getElementById("content").innerHTML = \'<div class="loading"><div class="spinner"></div></div>\';';
    html += 'try {';
    html += 'var params = new URLSearchParams();';
    html += 'if (state.filters.year) params.append("year", state.filters.year);';
    html += 'if (state.filters.fiscalYear) params.append("fiscalYear", state.filters.fiscalYear);';
    html += 'if (state.filters.customers.length > 0) params.append("customers", state.filters.customers.join(","));';
    html += 'if (state.filters.month) params.append("month", state.filters.month);';
    html += 'if (state.filters.commodity) params.append("commodity", state.filters.commodity);';
    html += 'if (state.filters.status) params.append("status", state.filters.status);';
    html += 'var url = state.view === "orders" ? "/api/orders/by-so?" : "/api/orders?";';
    html += 'var res = await fetch(url + params.toString());';
    html += 'var data = await res.json();';
    html += 'state.data = data;';
    html += 'updateStats(data.stats || {});';
    html += 'renderContent(data);';
    html += '} catch(e) { console.error("Error loading data:", e); document.getElementById("content").innerHTML = \'<div class="empty-state"><h3>Error loading data</h3><p>\' + e.message + \'</p></div>\'; }}';

    // Update stats
    html += 'function updateStats(stats) {';
    html += 'document.getElementById("statOrders").textContent = formatNumber(stats.order_count || 0);';
    html += 'document.getElementById("statCustomers").textContent = formatNumber(stats.customer_count || 0);';
    html += 'document.getElementById("statStyles").textContent = formatNumber(stats.style_count || 0);';
    html += 'document.getElementById("statUnits").textContent = formatNumber(stats.total_qty || 0);';
    html += 'document.getElementById("statDollars").textContent = formatNumber(Math.round(stats.total_dollars || 0));}';

    // Render content
    html += 'function renderContent(data) {';
    html += 'var container = document.getElementById("content");';
    html += 'try {';
    html += 'if (state.view === "monthly") { renderMonthlyView(container, data.items || []); }';
    html += 'else if (state.view === "summary") { renderSummaryView(container, data.items || []); }';
    html += 'else if (state.view === "styles") { renderStylesView(container, data.items || []); }';
    html += 'else if (state.view === "charts") { renderChartsView(container, data); }';
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
    html += 'var monthKey = o.delivery_date ? new Date(o.delivery_date).toISOString().slice(0,7) : "9999-99";';
    html += 'var monthLabel = o.delivery_date ? new Date(o.delivery_date).toLocaleDateString("en-US", {month: "long", year: "numeric"}) : "No Date";';
    html += 'var monthShort = o.delivery_date ? new Date(o.delivery_date).toLocaleDateString("en-US", {month: "short"}) : "TBD";';
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
    html += 'if (imgSrc) out += \'<img src="\' + imgSrc + \'" alt="" onerror="this.style.display=\\\'none\\\'">\';';
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
    html += 'html += \'<div class="style-card" onclick="showStyleDetail(\\\'\'+ item.style_number +\'\\\')"><div class="style-image">\';';
    html += 'if (item.commodity) html += \'<span class="commodity-badge">\' + escapeHtml(item.commodity) + \'</span>\';';
    html += 'html += \'<span class="style-badge">\' + item.order_count + \' order\' + (item.order_count > 1 ? "s" : "") + \'</span>\';';
    html += 'if (imgSrc) html += \'<img src="\' + imgSrc + \'" alt="" onerror="this.style.display=\\\'none\\\'">\';';
    html += 'else html += \'<span style="color:#ccc;font-size:3rem">👔</span>\';';
    html += 'html += \'</div><div class="style-info"><div class="style-number">\' + escapeHtml(item.style_number) + \'</div>\';';
    html += 'html += \'<div class="style-name">\' + escapeHtml(item.style_name || item.style_number) + \'</div>\';';
    html += 'html += \'<div class="style-stats"><div class="style-stat"><div class="style-stat-value">\' + formatNumber(item.total_qty) + \'</div><div class="style-stat-label">Units</div></div>\';';
    html += 'html += \'<div class="style-stat"><div class="style-stat-value money">\' + formatNumber(Math.round(item.total_dollars)) + \'</div><div class="style-stat-label">Value</div></div></div></div></div>\'; });';
    html += 'html += \'</div>\'; container.innerHTML = html; }';

    // Render summary view - matrix of commodities vs months
    html += 'function renderSummaryView(container, items) {';
    html += 'if (items.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3><p>Try adjusting your filters or import some data</p></div>\'; return; }';
    // Build data structure: commodities x months
    html += 'var data = {}; var months = new Set(); var commodities = new Set();';
    html += 'items.forEach(function(item) {';
    html += 'var comm = item.commodity || "Other";';
    html += 'commodities.add(comm);';
    html += 'item.orders.forEach(function(o) {';
    html += 'var monthKey = o.delivery_date ? new Date(o.delivery_date).toISOString().slice(0,7) : "TBD";';
    html += 'months.add(monthKey);';
    html += 'var key = comm + "|" + monthKey;';
    html += 'if (!data[key]) data[key] = { dollars: 0, units: 0 };';
    html += 'data[key].dollars += o.total_amount || 0;';
    html += 'data[key].units += o.quantity || 0;';
    html += '}); });';
    // Sort months and commodities
    html += 'var sortedMonths = Array.from(months).sort();';
    html += 'var commTotals = {};';
    html += 'Array.from(commodities).forEach(function(c) {';
    html += 'commTotals[c] = { dollars: 0, units: 0 };';
    html += 'sortedMonths.forEach(function(m) {';
    html += 'var key = c + "|" + m;';
    html += 'if (data[key]) { commTotals[c].dollars += data[key].dollars; commTotals[c].units += data[key].units; }';
    html += '}); });';
    html += 'var sortedComms = Array.from(commodities).sort(function(a,b) { return commTotals[b].dollars - commTotals[a].dollars; });';
    // Calculate column totals
    html += 'var colTotals = {};';
    html += 'sortedMonths.forEach(function(m) {';
    html += 'colTotals[m] = { dollars: 0, units: 0 };';
    html += 'sortedComms.forEach(function(c) {';
    html += 'var key = c + "|" + m;';
    html += 'if (data[key]) { colTotals[m].dollars += data[key].dollars; colTotals[m].units += data[key].units; }';
    html += '}); });';
    html += 'var grandTotal = { dollars: 0, units: 0 };';
    html += 'sortedComms.forEach(function(c) { grandTotal.dollars += commTotals[c].dollars; grandTotal.units += commTotals[c].units; });';
    // Build HTML
    html += 'var out = \'<div class="summary-container">\';';
    html += 'out += \'<div class="summary-legend"><span><div class="comm-color-box" style="background:#0088c2"></div>Top</span><span><div class="comm-color-box" style="background:#34c759"></div>Bottom</span><span><div class="comm-color-box" style="background:#af52de"></div>Dress</span><span><div class="comm-color-box" style="background:#ff9500"></div>Sweater</span><span><div class="comm-color-box" style="background:#ff3b30"></div>Jacket</span></div>\';';
    html += 'out += \'<table class="summary-table"><thead><tr><th>Commodity</th>\';';
    // Month headers
    html += 'sortedMonths.forEach(function(m) {';
    html += 'var label = m === "TBD" ? "TBD" : new Date(m + "-01").toLocaleDateString("en-US", {month: "short", year: "2-digit"});';
    html += 'out += \'<th>\' + label + \'</th>\'; });';
    html += 'out += \'<th class="row-total">Total</th></tr></thead><tbody>\';';
    // Data rows
    html += 'sortedComms.forEach(function(comm) {';
    html += 'var rowClass = "comm-row-" + comm.toLowerCase().replace(/[^a-z]/g, "");';
    html += 'if (["top","bottom","dress","sweater","jacket"].indexOf(comm.toLowerCase()) === -1) rowClass = "comm-row-other";';
    html += 'out += \'<tr class="\' + rowClass + \'"><td>\' + escapeHtml(comm) + \'</td>\';';
    html += 'sortedMonths.forEach(function(m) {';
    html += 'var key = comm + "|" + m;';
    html += 'var cell = data[key] || { dollars: 0, units: 0 };';
    html += 'var hasValue = cell.dollars > 0;';
    html += 'out += \'<td><div class="summary-cell\' + (hasValue ? " has-value" : "") + \'">\';';
    html += 'out += \'<span class="dollars">$\' + formatNumber(Math.round(cell.dollars)) + \'</span>\';';
    html += 'out += \'<span class="units">\' + formatNumber(cell.units) + \' units</span>\';';
    html += 'out += \'</div></td>\'; });';
    // Row total
    html += 'out += \'<td class="row-total"><div class="summary-cell has-value"><span class="dollars">$\' + formatNumber(Math.round(commTotals[comm].dollars)) + \'</span><span class="units">\' + formatNumber(commTotals[comm].units) + \' units</span></div></td></tr>\'; });';
    // Column totals row
    html += 'out += \'<tr class="col-total"><td><strong>Monthly Total</strong></td>\';';
    html += 'sortedMonths.forEach(function(m) {';
    html += 'out += \'<td><div class="summary-cell has-value"><span class="dollars">$\' + formatNumber(Math.round(colTotals[m].dollars)) + \'</span><span class="units">\' + formatNumber(colTotals[m].units) + \' units</span></div></td>\'; });';
    // Grand total
    html += 'out += \'<td class="grand-total"><div class="summary-cell"><span class="dollars" style="color:white">$\' + formatNumber(Math.round(grandTotal.dollars)) + \'</span><span class="units" style="color:rgba(255,255,255,0.7)">\' + formatNumber(grandTotal.units) + \' units</span></div></td></tr>\';';
    html += 'out += \'</tbody></table></div>\';';
    html += 'container.innerHTML = out; }';

    // Render orders view
    html += 'function renderOrdersView(container, orders) {';
    html += 'if (orders.length === 0) { container.innerHTML = \'<div class="empty-state"><h3>No orders found</h3><p>Try adjusting your filters or import some data</p></div>\'; return; }';
    html += 'var html = \'<div class="so-grid">\';';
    html += 'orders.forEach(function(order, idx) {';
    html += 'var deliveryDate = order.delivery_date ? new Date(order.delivery_date).toLocaleDateString("en-US", {month: "short", day: "numeric", year: "numeric"}) : "TBD";';
    html += 'html += \'<div class="so-card"><div class="so-header" onclick="toggleSO(\' + idx + \')">\';';
    html += 'html += \'<div class="so-info"><h3>SO# \' + escapeHtml(order.so_number) + \'</h3><span>\' + escapeHtml(order.customer) + \' · \' + order.line_count + \' items</span></div>\';';
    html += 'html += \'<div class="so-meta"><div class="delivery">\' + deliveryDate + \'</div><div class="total">$\' + formatNumber(Math.round(order.total_dollars)) + \'</div></div></div>\';';
    html += 'html += \'<div class="so-items" id="so-items-\' + idx + \'">\';';
    html += 'order.items.forEach(function(item) {';
    html += 'var imgSrc = item.image_url || "";';
    html += 'if (imgSrc.includes("workdrive.zoho.com") || imgSrc.includes("download-accl.zoho.com")) {';
    html += 'var match = imgSrc.match(/\\/download\\/([a-zA-Z0-9]+)/); if (match) imgSrc = "/api/image/" + match[1]; }';
    html += 'html += \'<div class="so-item"><div class="so-item-image">\';';
    html += 'if (imgSrc) html += \'<img src="\' + imgSrc + \'" alt="" onerror="this.style.display=\\\'none\\\'">\';';
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
    // Monthly trend data
    html += 'var monthlyData = {}; var customerData = {}; var commodityData = {};';
    html += 'items.forEach(function(item) {';
    html += 'item.orders.forEach(function(o) {';
    html += 'var monthKey = o.delivery_date ? new Date(o.delivery_date).toLocaleDateString("en-US", {month: "short", year: "2-digit"}) : "TBD";';
    html += 'if (!monthlyData[monthKey]) monthlyData[monthKey] = { units: 0, dollars: 0 };';
    html += 'monthlyData[monthKey].units += o.quantity || 0;';
    html += 'monthlyData[monthKey].dollars += o.total_amount || 0;';
    html += 'var cust = o.customer || "Unknown";';
    html += 'if (!customerData[cust]) customerData[cust] = 0;';
    html += 'customerData[cust] += o.total_amount || 0;';
    html += 'var comm = item.commodity || "Other";';
    html += 'if (!commodityData[comm]) commodityData[comm] = 0;';
    html += 'commodityData[comm] += o.total_amount || 0;';
    html += '}); });';
    // Monthly chart placeholder
    html += 'html += \'<div class="chart-card"><h3>📈 Monthly Trend</h3><div class="chart-wrapper"><canvas id="monthlyChart"></canvas></div></div>\';';
    // Customer pie chart placeholder
    html += 'html += \'<div class="chart-card"><h3>👥 Top Customers</h3><div class="chart-wrapper"><canvas id="customerChart"></canvas></div></div>\';';
    // Commodity chart placeholder
    html += 'html += \'<div class="chart-card"><h3>📦 By Commodity</h3><div class="chart-wrapper"><canvas id="commodityChart"></canvas></div></div>\';';
    html += 'html += \'</div>\';';
    html += 'container.innerHTML = html;';
    // Render charts
    html += 'setTimeout(function() { renderCharts(monthlyData, customerData, commodityData); }, 100); }';

    // Render charts with Chart.js
    html += 'function renderCharts(monthlyData, customerData, commodityData) {';
    html += 'var months = Object.keys(monthlyData);';
    html += 'var monthlyDollars = months.map(function(m) { return Math.round(monthlyData[m].dollars); });';
    // Monthly bar chart
    html += 'new Chart(document.getElementById("monthlyChart"), { type: "bar", data: { labels: months, datasets: [{ label: "Dollars", data: monthlyDollars, backgroundColor: "#0088c2" }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { display: false } }, scales: { y: { beginAtZero: true, ticks: { callback: function(v) { return "$" + (v/1000).toFixed(0) + "K"; } } } } } });';
    // Customer pie chart - top 6
    html += 'var custEntries = Object.entries(customerData).sort(function(a,b) { return b[1] - a[1]; }).slice(0, 6);';
    html += 'var custLabels = custEntries.map(function(e) { return e[0]; });';
    html += 'var custValues = custEntries.map(function(e) { return Math.round(e[1]); });';
    html += 'var colors = ["#1e3a5f", "#0088c2", "#4da6d9", "#86868b", "#c7d1d9", "#f5f5f7"];';
    html += 'new Chart(document.getElementById("customerChart"), { type: "doughnut", data: { labels: custLabels, datasets: [{ data: custValues, backgroundColor: colors }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: "right" } } } });';
    // Commodity pie chart
    html += 'var commEntries = Object.entries(commodityData).sort(function(a,b) { return b[1] - a[1]; });';
    html += 'var commLabels = commEntries.map(function(e) { return e[0]; });';
    html += 'var commValues = commEntries.map(function(e) { return Math.round(e[1]); });';
    html += 'new Chart(document.getElementById("commodityChart"), { type: "doughnut", data: { labels: commLabels, datasets: [{ data: commValues, backgroundColor: colors.concat(["#2d5a87", "#66b3d9", "#003d5c"]) }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: "right" } } } });';
    html += '}';

    // Multi-select functions
    html += 'function toggleMultiSelect(type) {';
    html += 'var dropdown = document.getElementById(type + "Dropdown");';
    html += 'dropdown.classList.toggle("open");';
    html += 'document.addEventListener("click", function closeDropdown(e) { if (!e.target.closest(".multi-select")) { dropdown.classList.remove("open"); document.removeEventListener("click", closeDropdown); } }); }';

    html += 'function updateCustomerFilter() {';
    html += 'var checkboxes = document.querySelectorAll("#customerDropdown input[type=checkbox]:checked");';
    html += 'state.filters.customers = Array.from(checkboxes).map(function(cb) { return cb.value; });';
    html += 'var display = document.getElementById("customerDisplay");';
    html += 'if (state.filters.customers.length === 0) display.textContent = "All Customers";';
    html += 'else if (state.filters.customers.length === 1) display.textContent = state.filters.customers[0];';
    html += 'else display.textContent = state.filters.customers.length + " selected";';
    html += 'updateClearButton();';
    html += 'loadData(); }';

    html += 'function selectAllCustomers() {';
    html += 'document.querySelectorAll("#customerDropdown input[type=checkbox]").forEach(function(cb) { cb.checked = true; });';
    html += 'updateCustomerFilter(); }';

    html += 'function clearAllCustomers() {';
    html += 'document.querySelectorAll("#customerDropdown input[type=checkbox]").forEach(function(cb) { cb.checked = false; });';
    html += 'updateCustomerFilter(); }';

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
    html += 'ordersHtml += \'<div class="order-row"><div class="order-row-left"><div class="order-row-so">SO# \' + escapeHtml(o.so_number) + \'</div><div class="order-row-customer">\' + escapeHtml(o.customer) + (o.color ? " · " + escapeHtml(o.color) : "") + \'</div></div>\';';
    html += 'ordersHtml += \'<div class="order-row-right"><div class="order-row-qty">\' + formatNumber(o.quantity) + \' units</div><div class="order-row-date">\' + date + \'</div></div></div>\'; });';
    html += 'document.getElementById("modalOrdersList").innerHTML = ordersHtml;';
    html += 'document.getElementById("styleModal").classList.add("active"); }';

    // Close modal
    html += 'function closeModal() { document.getElementById("styleModal").classList.remove("active"); }';

    // Upload modal
    html += 'function showUploadModal() { document.getElementById("uploadModal").classList.add("active"); }';
    html += 'function closeUploadModal() { document.getElementById("uploadModal").classList.remove("active"); document.getElementById("uploadProgress").classList.remove("active"); }';

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

    // Filter handlers
    html += 'document.getElementById("yearFilter").addEventListener("change", function(e) { state.filters.year = e.target.value; state.filters.fiscalYear = ""; document.getElementById("fiscalYearFilter").value = ""; document.getElementById("fiscalYearFilter").classList.remove("active"); e.target.classList.toggle("active", !!e.target.value); updateClearButton(); loadData(); });';
    html += 'document.getElementById("fiscalYearFilter").addEventListener("change", function(e) { state.filters.fiscalYear = e.target.value; state.filters.year = ""; document.getElementById("yearFilter").value = ""; document.getElementById("yearFilter").classList.remove("active"); e.target.classList.toggle("active", !!e.target.value); updateClearButton(); loadData(); });';
    html += 'document.getElementById("monthFilter").addEventListener("change", function(e) { state.filters.month = e.target.value; e.target.classList.toggle("active", !!e.target.value); updateClearButton(); loadData(); });';
    html += 'document.getElementById("commodityFilter").addEventListener("change", function(e) { state.filters.commodity = e.target.value; e.target.classList.toggle("active", !!e.target.value); updateClearButton(); loadData(); });';

    // Status toggle
    html += 'document.querySelectorAll(".status-btn").forEach(function(btn) { btn.addEventListener("click", function() {';
    html += 'document.querySelectorAll(".status-btn").forEach(function(b) { b.classList.remove("active"); });';
    html += 'btn.classList.add("active"); state.filters.status = btn.dataset.status; loadData(); }); });';

    // View toggle
    html += 'document.querySelectorAll(".view-btn").forEach(function(btn) { btn.addEventListener("click", function() {';
    html += 'document.querySelectorAll(".view-btn").forEach(function(b) { b.classList.remove("active"); });';
    html += 'btn.classList.add("active"); state.view = btn.dataset.view; loadData(); }); });';

    // Clear filters
    html += 'function clearFilters() {';
    html += 'state.filters = { year: "", fiscalYear: "", customers: [], month: "", commodity: "", status: state.filters.status };';
    html += 'document.getElementById("yearFilter").value = ""; document.getElementById("yearFilter").classList.remove("active");';
    html += 'document.getElementById("fiscalYearFilter").value = ""; document.getElementById("fiscalYearFilter").classList.remove("active");';
    html += 'document.querySelectorAll("#customerDropdown input[type=checkbox]").forEach(function(cb) { cb.checked = false; });';
    html += 'document.getElementById("customerDisplay").textContent = "All Customers";';
    html += 'document.getElementById("monthFilter").value = ""; document.getElementById("monthFilter").classList.remove("active");';
    html += 'document.getElementById("commodityFilter").value = ""; document.getElementById("commodityFilter").classList.remove("active");';
    html += 'updateClearButton(); loadData(); }';
    html += 'function updateClearButton() { var hasFilters = state.filters.customers.length > 0 || state.filters.month || state.filters.commodity || state.filters.year || state.filters.fiscalYear;';
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
    html += 'if (filters.customer) { state.filters.customer = filters.customer; document.getElementById("customerFilter").value = filters.customer; document.getElementById("customerFilter").classList.add("active"); }';
    html += 'if (filters.commodity) { state.filters.commodity = filters.commodity; document.getElementById("commodityFilter").value = filters.commodity; document.getElementById("commodityFilter").classList.add("active"); }';
    html += 'if (filters.month) { state.filters.month = filters.month; document.getElementById("monthFilter").value = filters.month; document.getElementById("monthFilter").classList.add("active"); }';
    html += 'updateClearBtn(); loadData(); }';

    // Helpers
    html += 'function formatNumber(n) { return n.toString().replace(/\\B(?=(\\d{3})+(?!\\d))/g, ","); }';
    html += 'function escapeHtml(str) { if (!str) return ""; return str.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;"); }';

    // Modal click outside
    html += 'document.getElementById("styleModal").addEventListener("click", function(e) { if (e.target === this) closeModal(); });';
    html += 'document.getElementById("uploadModal").addEventListener("click", function(e) { if (e.target === this) closeUploadModal(); });';

    // Init
    html += 'loadFilters(); loadData();';

    html += '</script></body></html>';
    return html;
}

// Start server
initDB().then(function() {
    app.listen(PORT, function() {
        console.log('Open Orders Dashboard running on port ' + PORT);
    });
});
