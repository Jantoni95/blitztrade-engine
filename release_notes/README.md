Release notes for desktop auto-updates live here.

Create one JSON file per release tag before pushing the tag.

Example for tag v1.2.3:

{
  "summary": "Faster order handling and cleaner local testing.",
  "release_notes": [
    "Stopped duplicate order rows after order submission.",
    "Added local dev terminal mode so server logs stay visible while testing.",
    "Fixed Windows startup to use the repo virtualenv reliably."
  ]
}

Save that as:

release_notes/v1.2.3.json

The release workflow will embed these notes into releases/latest.json, and the app updater will show them in the update dialog.