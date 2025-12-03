# Setup Instructions

## Quick Start

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd migtation-script
   ```

2. **Create virtual environment**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

4. **Configure Airtable credentials**
   ```bash
   cp .env.example .env
   ```

   Then edit `.env` and add your Airtable API key:
   ```
   AIRTABLE_API_KEY=your_actual_api_key_here
   ```

   **Where to get your API key:**
   - Go to https://airtable.com/create/tokens
   - Create a new token with access to your base
   - Copy and paste it into `.env`

5. **Run the app**
   ```bash
   streamlit run app.py
   ```

## Features

- ✅ **Auto-loads from Airtable** - Data loaded automatically on startup
- ✅ **Auto-sync approvals** - Every approval syncs to Airtable immediately
- ✅ **Real-time collaboration** - All team members see the same data
- ✅ **CSV backups** - Local backups created automatically

## Airtable Configuration

The following are already configured (no need to change):
- **Base ID**: `appt1H2lJxpR8NCbC`
- **Mapping Table**: `tbl7xPdfPcPKzx3Tc`
- **Approvals Table**: `tblWWegam2OOTYpv3`

You only need to provide your **API key** in the `.env` file.

## Troubleshooting

**App shows "Airtable credentials not configured"**
- Make sure you created `.env` file by copying `.env.example`
- Check that `AIRTABLE_API_KEY` is filled in with a valid key

**Approvals not syncing**
- Check the sidebar - it should show "✅ Airtable connected"
- Check that your API key has write access to the Approvals table

**Data not loading**
- Go to "Data Sources" → "Airtable" → Click "🔄 Force Refresh from Airtable Now"
- Check console for error messages
