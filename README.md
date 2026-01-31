# Open Orders Dashboard

Visual sales order dashboard for the Mark Edwards Apparel sales team. View open orders by customer, delivery date, and commodity with AI-powered natural language search.

## Features

- **Visual Style Cards** - See styles on order with images, quantities, and dollar values
- **Smart Filters** - Filter by customer, delivery month, commodity, and status
- **Two Views** - View by Style (grouped) or by Sales Order number
- **AI Chat** - Ask natural language questions like "Show me Burlington's denim orders for June"
- **CSV Import** - Easy data import from Zoho Books export
- **WorkDrive Images** - Automatic image loading from Zoho WorkDrive links

## Tech Stack

- Node.js / Express
- PostgreSQL
- Anthropic Claude API (for AI chat)
- Zoho OAuth (for WorkDrive images)

## Environment Variables

```env
# Required
DATABASE_URL=postgresql://user:pass@host:5432/dbname
PORT=3001

# For AI chat
ANTHROPIC_API_KEY=sk-ant-...

# For WorkDrive images
ZOHO_CLIENT_ID=your_client_id
ZOHO_CLIENT_SECRET=your_client_secret
ZOHO_REFRESH_TOKEN=your_refresh_token

# Optional - link to product catalog
PRODUCT_CATALOG_URL=https://your-product-catalog.railway.app

# Optional - for Railway volume mount
IMAGE_CACHE_DIR=/cache
```

## CSV Import Format

The import accepts flexible column names. Expected columns:

| Field | Accepted Names |
|-------|---------------|
| SO Number | `so_number`, `so`, `sales_order`, `order_number` |
| Customer | `customer`, `customer_name`, `buyer`, `account` |
| Style Number | `style_number`, `style`, `sku`, `item` |
| Style Name | `style_name`, `name`, `description` |
| Commodity | `commodity`, `category`, `fabric`, `type` |
| Color | `color`, `colour`, `colorway` |
| Quantity | `quantity`, `qty`, `units` |
| Unit Price | `unit_price`, `price`, `rate` |
| Total Amount | `total_amount`, `total`, `amount` |
| Delivery Date | `delivery_date`, `ship_date`, `due_date`, `eta` |
| Status | `status` (values: Open, Invoiced/Shipped/Complete) |
| Image URL | `image_url`, `image`, `workdrive_link` |

## Local Development

```bash
npm install
npm run dev
```

## Deploy to Railway

1. Create new project on Railway
2. Add PostgreSQL database
3. Connect GitHub repo
4. Set environment variables
5. Deploy

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `GET /api/orders` | Get order items grouped by style |
| `GET /api/orders/by-so` | Get orders grouped by SO number |
| `GET /api/filters` | Get available filter options |
| `GET /api/dashboard` | Get dashboard summary stats |
| `POST /api/import` | Import CSV file |
| `POST /api/chat` | AI chat endpoint |
| `GET /api/image/:fileId` | Proxy WorkDrive images |

## Related Apps

- [Product Catalog](https://github.com/markedwards8480/product-catalog-) - Inventory availability viewer
