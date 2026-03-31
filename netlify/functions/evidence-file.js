const fs = require("fs");
const path = require("path");
const https = require("https");

function loadDotEnv() {
  const dotenvPath = path.join(process.cwd(), ".env");
  if (!fs.existsSync(dotenvPath)) return;
  const lines = fs.readFileSync(dotenvPath, "utf8").split(/\r?\n/);
  for (const rawLine of lines) {
    const line = String(rawLine || "").trim();
    if (!line || line.startsWith("#")) continue;
    const index = line.indexOf("=");
    if (index <= 0) continue;
    const key = line.slice(0, index).trim();
    const value = line.slice(index + 1).trim().replace(/^['"]|['"]$/g, "");
    if (key && process.env[key] == null) {
      process.env[key] = value;
    }
  }
}

loadDotEnv();

function redirect(location) {
  return {
    statusCode: 302,
    headers: {
      Location: location,
      "cache-control": "no-store",
    },
    body: "",
  };
}

function text(statusCode, body) {
  return {
    statusCode,
    headers: { "content-type": "text/plain; charset=utf-8" },
    body: String(body || ""),
  };
}

function isCompaniesHouseDocumentUrl(rawUrl) {
  try {
    const url = new URL(String(rawUrl || "").trim());
    return /(^|\.)document-api\.company-information\.service\.gov\.uk$/i.test(url.hostname)
      && /^\/document\/.+/.test(url.pathname);
  } catch (_error) {
    return false;
  }
}

function appendPageFragment(rawUrl, page) {
  const pageNumber = Number(page || 0);
  if (!pageNumber || !Number.isFinite(pageNumber)) return rawUrl;
  return `${rawUrl}#page=${pageNumber}`;
}

function fetchRedirectLocation(url, headers) {
  return new Promise((resolve, reject) => {
    const request = https.request(
      url,
      {
        method: "GET",
        headers,
      },
      (response) => {
        response.resume();
        const location = response.headers.location;
        if (![301, 302, 303, 307, 308].includes(response.statusCode || 0)) {
          reject(new Error(`Companies House document API returned ${response.statusCode || 0}`));
          return;
        }
        if (!location) {
          reject(new Error("Companies House document redirect had no Location header"));
          return;
        }
        resolve(location);
      }
    );
    request.on("error", reject);
    request.end();
  });
}

exports.handler = async function handler(event) {
  if (event.httpMethod !== "GET") {
    return text(405, "Method not allowed.");
  }

  const rawUrl = String(event.queryStringParameters?.url || "").trim();
  const page = String(event.queryStringParameters?.page || "").trim();
  if (!rawUrl) {
    return text(400, "Missing url parameter.");
  }

  try {
    new URL(rawUrl);
  } catch (_error) {
    return text(400, "Invalid url parameter.");
  }

  if (!isCompaniesHouseDocumentUrl(rawUrl)) {
    return redirect(appendPageFragment(rawUrl, page));
  }

  const apiKey = String(process.env.COMPANIES_HOUSE_API_KEY || "").trim();
  if (!apiKey) {
    return text(500, "COMPANIES_HOUSE_API_KEY is not configured.");
  }

  const contentUrl = new URL(rawUrl);
  contentUrl.pathname = `${contentUrl.pathname.replace(/\/$/, "")}/content`;

  try {
    const location = await fetchRedirectLocation(contentUrl, {
      Authorization: `Basic ${Buffer.from(`${apiKey}:`).toString("base64")}`,
      Accept: "application/pdf",
      "User-Agent": process.env.USER_AGENT || "project-istari/0.1",
    });
    return redirect(appendPageFragment(location, page));
  } catch (error) {
    return text(502, error.message || "Failed to resolve evidence file.");
  }
};
