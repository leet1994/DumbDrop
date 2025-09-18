require('dotenv').config();

const { validatePin } = require('../utils/security');
const logger = require('../utils/logger');
const fs = require('fs');
const path = require('path');
const { version } = require('../../package.json'); // Get version from package.json

/**
 * Environment Variables Reference
 *
 * PORT                - Port for the server (default: 3000)
 * NODE_ENV            - Node environment (default: 'development')
 * BASE_URL            - Base URL for the app (default: http://localhost:${PORT})
 * UPLOAD_DIR          - Directory for uploads (Docker/production)
 * LOCAL_UPLOAD_DIR    - Directory for uploads (local dev, fallback: './local_uploads')
 * MAX_FILE_SIZE       - Max upload size in MB (default: 1024)
 * AUTO_UPLOAD         - Enable auto-upload (true/false, default: false)
 * SHOW_FILE_LIST      - Enable file listing in frontend (true/false, default: false)
 * DUMBDROP_PIN        - Security PIN for uploads (required for protected endpoints)
 * DUMBDROP_TITLE      - Site title (default: 'DumbDrop')
 * APPRISE_URL         - Apprise notification URL (optional)
 * APPRISE_MESSAGE     - Notification message template (default provided)
 * APPRISE_SIZE_UNIT   - Size unit for notifications (optional)
 * ALLOWED_EXTENSIONS  - Comma-separated list of allowed file extensions (optional)
 */

// Helper for clear configuration logging
const logConfig = (message, level = 'info') => {
  const prefix = level === 'warning' ? '⚠️ WARNING:' : 'ℹ️ INFO:';
  console.log(`${prefix} CONFIGURATION: ${message}`);
};

// Default configurations
const DEFAULT_CHUNK_SIZE = 1024 * 1024 * 100; // 100MB
const DEFAULT_SITE_TITLE = 'DumbDrop';
const NODE_ENV = process.env.NODE_ENV || 'production';
const PORT = process.env.PORT || 3000;
const BASE_URL = process.env.BASE_URL || `http://localhost:${PORT}`;
const DEFAULT_CLIENT_MAX_RETRIES = 5; // Default retry count
console.log('Loaded ENV:', {
  PORT,
  UPLOAD_DIR: process.env.UPLOAD_DIR,
  LOCAL_UPLOAD_DIR: process.env.LOCAL_UPLOAD_DIR,
  NODE_ENV,
  BASE_URL,
  ALLOWED_ORIGINS: process.env.ALLOWED_ORIGINS || '*',
});
const logAndReturn = (key, value, isDefault = false) => {
  logConfig(`${key}: ${value}${isDefault ? ' (default)' : ''}`);
  return value;
};

/**
 * Determine the upload directory based on environment variables.
 * Priority:
 *   1. UPLOAD_DIR (for Docker/production)
 *   2. LOCAL_UPLOAD_DIR (for local development)
 *   3. './local_uploads' (default fallback)
 * @returns {string} The upload directory path
 */
function determineUploadDirectory() {
  let uploadDir;
  if (process.env.UPLOAD_DIR) {
    uploadDir = process.env.UPLOAD_DIR;
    logConfig(`Upload directory set from UPLOAD_DIR: ${uploadDir}`);
  } else if (process.env.LOCAL_UPLOAD_DIR) {
    uploadDir = process.env.LOCAL_UPLOAD_DIR;
    logConfig(`Upload directory using LOCAL_UPLOAD_DIR fallback: ${uploadDir}`, 'warning');
  } else {
    uploadDir = './local_uploads';
    logConfig(`Upload directory using default fallback: ${uploadDir}`, 'warning');
  }
  logConfig(`Final upload directory path: ${require('path').resolve(uploadDir)}`);
  return uploadDir;
}

/**
 * Utility to detect if running in local development mode
 * Returns true if NODE_ENV is not 'production' and UPLOAD_DIR is not set (i.e., not Docker)
 */
function isLocalDevelopment() {
  return process.env.NODE_ENV !== 'production';
}

/**
 * Ensure the upload directory exists (for local development only)
 * Creates the directory if it does not exist
 */
function ensureLocalUploadDirExists(uploadDir) {
  if (!isLocalDevelopment()) return;
  try {
    if (!fs.existsSync(uploadDir)) {
      fs.mkdirSync(uploadDir, { recursive: true });
      logConfig(`Created local upload directory: ${uploadDir}`);
    } else {
      logConfig(`Local upload directory exists: ${uploadDir}`);
    }
  } catch (err) {
    logConfig(`Failed to create local upload directory: ${uploadDir}. Error: ${err.message}`, 'warning');
  }
}

// Determine and ensure upload directory (for local dev)
const resolvedUploadDir = determineUploadDirectory();
ensureLocalUploadDirExists(resolvedUploadDir);

/**
 * Application configuration
 * Loads and validates environment variables
 */
const config = {
  // =====================
  // =====================
  // Server settings
  // =====================
  /**
   * Port for the server (default: 3000)
   * Set via PORT in .env
   */
  port: PORT,
  /**
   * Node environment (default: 'production')
   * Set via NODE_ENV in .env
   */
  nodeEnv: NODE_ENV,
  /**
   * Base URL for the app (default: http://localhost:${PORT})
   * Set via BASE_URL in .env
   */
  baseUrl: BASE_URL,
  
  // =====================
  // =====================
  // Upload settings
  // =====================
  /**
   * Directory for uploads
   * Priority: UPLOAD_DIR (Docker/production) > LOCAL_UPLOAD_DIR (local dev) > './local_uploads' (fallback)
   */
  uploadDir: resolvedUploadDir,
  /**
   * Max upload size in bytes (default: 1024MB)
   * Set via MAX_FILE_SIZE in .env (in MB)
   */
  maxFileSize: (() => {
    const sizeInMB = parseInt(process.env.MAX_FILE_SIZE || '1024', 10);
    if (isNaN(sizeInMB) || sizeInMB <= 0) {
      throw new Error('MAX_FILE_SIZE must be a positive number');
    }
    return sizeInMB * 1024 * 1024; // Convert MB to bytes
  })(),
  /**
   * Enable auto-upload (true/false, default: false)
   * Set via AUTO_UPLOAD in .env
   */
  autoUpload: process.env.AUTO_UPLOAD === 'true',
  /**
   * Enable file listing in frontend (true/false, default: false)
   * Set via SHOW_FILE_LIST in .env
   */
  showFileList: process.env.SHOW_FILE_LIST === 'true',
  
  // =====================
  // =====================
  // Security
  // =====================
  /**
   * Security PIN for uploads (required for protected endpoints)
   * Set via DUMBDROP_PIN in .env
   */
  pin: validatePin(process.env.DUMBDROP_PIN),
  
  // =====================
  // =====================
  // UI settings
  // =====================
  /**
   * Site title (default: 'DumbDrop')
   * Set via DUMBDROP_TITLE in .env
   */
  siteTitle: process.env.DUMBDROP_TITLE || DEFAULT_SITE_TITLE,
  
  // =====================
  // =====================
  // Notification settings
  // =====================
  /**
   * Apprise notification URL (optional)
   * Set via APPRISE_URL in .env
   */
  appriseUrl: process.env.APPRISE_URL,
  /**
   * Notification message template (default provided)
   * Set via APPRISE_MESSAGE in .env
   */
  appriseMessage: process.env.APPRISE_MESSAGE || 'New file uploaded - {filename} ({size}), Storage used {storage}',
  /**
   * Size unit for notifications (optional)
   * Set via APPRISE_SIZE_UNIT in .env
   */
  appriseSizeUnit: process.env.APPRISE_SIZE_UNIT,
  
  // =====================
  // =====================
  // File extensions
  // =====================
  /**
   * Allowed file extensions (comma-separated, optional)
   * Set via ALLOWED_EXTENSIONS in .env
   */
  allowedExtensions: process.env.ALLOWED_EXTENSIONS ? 
    process.env.ALLOWED_EXTENSIONS.split(',').map(ext => ext.trim().toLowerCase()) : 
    null,

  /**
   * Max number of retries for client-side chunk uploads (default: 5)
   * Set via CLIENT_MAX_RETRIES in .env
   */
  clientMaxRetries: (() => {
    const envValue = process.env.CLIENT_MAX_RETRIES;
    const defaultValue = DEFAULT_CLIENT_MAX_RETRIES;
    if (envValue === undefined) {
      return logAndReturn('CLIENT_MAX_RETRIES', defaultValue, true);
    }
    const retries = parseInt(envValue, 10);
    if (isNaN(retries) || retries < 0) {
      logConfig(
        `Invalid CLIENT_MAX_RETRIES value: "${envValue}". Using default: ${defaultValue}`,
        'warning',
      );
      return logAndReturn('CLIENT_MAX_RETRIES', defaultValue, true);
    }
    return logAndReturn('CLIENT_MAX_RETRIES', retries);
  })(),

  uploadPin: logAndReturn('UPLOAD_PIN', process.env.UPLOAD_PIN || null),
};

console.log(`Upload directory configured as: ${config.uploadDir}`);

// Validate required settings
function validateConfig() {
  const errors = [];
  
  if (config.maxFileSize <= 0) {
    errors.push('MAX_FILE_SIZE must be greater than 0');
  }

  // Validate BASE_URL format
  try {
    // Ensure BASE_URL ends with a slash
    if (!config.baseUrl.endsWith('/')) {
      logger.warn('BASE_URL did not end with a trailing slash. Automatically appending "/".');
      config.baseUrl = config.baseUrl + '/';
    }
  } catch (err) {
    errors.push('BASE_URL must be a valid URL');
  }
  
  if (config.nodeEnv === 'production') {
    if (!config.appriseUrl) {
      logger.info('Notifications disabled - No Configuration');
    }
  }
  
  if (errors.length > 0) {
    throw new Error('Configuration validation failed:\n' + errors.join('\n'));
  }
}

// Freeze configuration to prevent modifications
Object.freeze(config);

module.exports = {
  config,
  validateConfig
}; 