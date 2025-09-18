/**
 * File management and listing route handlers.
 * Provides endpoints for listing, downloading, and managing uploaded files.
 * Handles file metadata, stats, and directory operations.
 */

const express = require('express');
const router = express.Router();
const path = require('path');
const fs = require('fs').promises;
const { config } = require('../config');
const logger = require('../utils/logger');
const { formatFileSize } = require('../utils/fileUtils');

/**
 * Get file information
 */
router.get('/info/*', async (req, res) => {
  const filePath = path.join(config.uploadDir, req.params[0]);
  
  try {
    // Ensure the path is within the upload directory (security check)
    const resolvedFilePath = path.resolve(filePath);
    const resolvedUploadDir = path.resolve(config.uploadDir);
    if (!resolvedFilePath.startsWith(resolvedUploadDir)) {
      logger.warn(`Attempted path traversal attack: ${req.params[0]}`);
      return res.status(403).json({ error: 'Access denied' });
    }
    
    const stats = await fs.stat(filePath);
    const fileInfo = {
      filename: req.params[0],
      size: stats.size,
      formattedSize: formatFileSize(stats.size),
      uploadDate: stats.mtime,
      mimetype: path.extname(req.params[0]).slice(1),
      type: stats.isDirectory() ? 'directory' : 'file'
    };

    res.json(fileInfo);
  } catch (err) {
    logger.error(`Failed to get file info: ${err.message}`);
    res.status(404).json({ error: 'File not found' });
  }
});

/**
 * Download file
 */
router.get('/download/*', async (req, res) => {
  // Get the file path from the wildcard parameter
  const filePath = path.join(config.uploadDir, req.params[0]);
  const fileName = path.basename(req.params[0]);
  
  try {
    await fs.access(filePath);
    
    // Ensure the file is within the upload directory (security check)
    const resolvedFilePath = path.resolve(filePath);
    const resolvedUploadDir = path.resolve(config.uploadDir);
    if (!resolvedFilePath.startsWith(resolvedUploadDir)) {
      logger.warn(`Attempted path traversal attack: ${req.params[0]}`);
      return res.status(403).json({ error: 'Access denied' });
    }
    
    // Set headers for download
    res.setHeader('Content-Disposition', `attachment; filename="${fileName}"`);
    res.setHeader('Content-Type', 'application/octet-stream');
    
    // Stream the file
    const fileStream = require('fs').createReadStream(filePath);
    fileStream.pipe(res);
    
    // Handle errors during streaming
    fileStream.on('error', (err) => {
      logger.error(`File streaming error: ${err.message}`);
      if (!res.headersSent) {
        res.status(500).json({ error: 'Failed to download file' });
      }
    });
    
    logger.info(`File download started: ${req.params[0]}`);
  } catch (err) {
    logger.error(`File download failed: ${err.message}`);
    res.status(404).json({ error: 'File not found' });
  }
});

/**
 * List all files and folders recursively
 */
router.get('/', async (req, res) => {
  try {
    const items = await getDirectoryContents(config.uploadDir);
    
    // Calculate total size across all files
    const totalSize = calculateTotalSize(items);
    
    res.json({ 
      items: items,
      totalFiles: countFiles(items),
      totalSize: totalSize,
      formattedTotalSize: formatFileSize(totalSize)
    });
  } catch (err) {
    logger.error(`Failed to list files: ${err.message}`);
    res.status(500).json({ error: 'Failed to list files' });
  }
});

/**
 * Recursively get directory contents
 */
async function getDirectoryContents(dirPath, relativePath = '') {
  const items = [];
  
  try {
    const entries = await fs.readdir(dirPath);
    
    for (const entry of entries) {
      // Skip metadata directory and hidden files
      if (entry === '.metadata' || entry.startsWith('.')) {
        continue;
      }
      
      const fullPath = path.join(dirPath, entry);
      const itemRelativePath = relativePath ? `${relativePath}/${entry}` : entry;
      
      try {
        const stats = await fs.stat(fullPath);
        
        if (stats.isDirectory()) {
          const subItems = await getDirectoryContents(fullPath, itemRelativePath);
          items.push({
            name: entry,
            type: 'directory',
            path: itemRelativePath,
            size: calculateTotalSize(subItems),
            formattedSize: formatFileSize(calculateTotalSize(subItems)),
            uploadDate: stats.mtime,
            children: subItems
          });
        } else if (stats.isFile()) {
          items.push({
            name: entry,
            type: 'file',
            path: itemRelativePath,
            size: stats.size,
            formattedSize: formatFileSize(stats.size),
            uploadDate: stats.mtime,
            extension: path.extname(entry).toLowerCase()
          });
        }
      } catch (statErr) {
        logger.error(`Failed to get stats for ${fullPath}: ${statErr.message}`);
        continue;
      }
    }
    
    // Sort items: directories first, then files, both alphabetically
    items.sort((a, b) => {
      if (a.type !== b.type) {
        return a.type === 'directory' ? -1 : 1;
      }
      return a.name.localeCompare(b.name);
    });
    
  } catch (err) {
    logger.error(`Failed to read directory ${dirPath}: ${err.message}`);
  }
  
  return items;
}

/**
 * Calculate total size of all files in a directory structure
 */
function calculateTotalSize(items) {
  return items.reduce((total, item) => {
    if (item.type === 'file') {
      return total + item.size;
    } else if (item.type === 'directory' && item.children) {
      return total + calculateTotalSize(item.children);
    }
    return total;
  }, 0);
}

/**
 * Count total number of files in a directory structure
 */
function countFiles(items) {
  return items.reduce((count, item) => {
    if (item.type === 'file') {
      return count + 1;
    } else if (item.type === 'directory' && item.children) {
      return count + countFiles(item.children);
    }
    return count;
  }, 0);
}

/**
 * Delete file or directory
 */
router.delete('/*', async (req, res) => {
  // Get the file/directory path from the wildcard parameter
  const itemPath = path.join(config.uploadDir, req.params[0]);
  
  try {
    // Ensure the path is within the upload directory (security check)
    const resolvedItemPath = path.resolve(itemPath);
    const resolvedUploadDir = path.resolve(config.uploadDir);
    if (!resolvedItemPath.startsWith(resolvedUploadDir)) {
      logger.warn(`Attempted path traversal attack: ${req.params[0]}`);
      return res.status(403).json({ error: 'Access denied' });
    }
    
    await fs.access(itemPath);
    const stats = await fs.stat(itemPath);
    
    if (stats.isDirectory()) {
      // Delete directory recursively
      await fs.rm(itemPath, { recursive: true, force: true });
      logger.info(`Directory deleted: ${req.params[0]}`);
      res.json({ message: 'Directory deleted successfully' });
    } else {
      // Delete file
      await fs.unlink(itemPath);
      logger.info(`File deleted: ${req.params[0]}`);
      res.json({ message: 'File deleted successfully' });
    }
  } catch (err) {
    logger.error(`Deletion failed: ${err.message}`);
    res.status(err.code === 'ENOENT' ? 404 : 500).json({ 
      error: err.code === 'ENOENT' ? 'File or directory not found' : 'Failed to delete item' 
    });
  }
});

/**
 * Rename file or directory
 */
router.put('/rename/*', async (req, res) => {
  const { newName } = req.body;
  
  if (!newName || typeof newName !== 'string' || newName.trim() === '') {
    return res.status(400).json({ error: 'New name is required' });
  }
  
  // Get the current file/directory path from the wildcard parameter
  const currentPath = path.join(config.uploadDir, req.params[0]);
  const currentDir = path.dirname(currentPath);
  
  try {
    // Ensure the current path is within the upload directory (security check)
    const resolvedCurrentPath = path.resolve(currentPath);
    const resolvedUploadDir = path.resolve(config.uploadDir);
    if (!resolvedCurrentPath.startsWith(resolvedUploadDir)) {
      logger.warn(`Attempted path traversal attack: ${req.params[0]}`);
      return res.status(403).json({ error: 'Access denied' });
    }
    
    // Check if the current file/directory exists
    await fs.access(currentPath);
    const stats = await fs.stat(currentPath);
    
    // Sanitize the new name using our safe sanitization function
    const { sanitizeFilenameSafe } = require('../utils/fileUtils');
    const sanitizedNewName = sanitizeFilenameSafe(newName.trim());
    
    // Construct the new path
    const newPath = path.join(currentDir, sanitizedNewName);
    
    // Ensure the new path is also within the upload directory
    const resolvedNewPath = path.resolve(newPath);
    if (!resolvedNewPath.startsWith(resolvedUploadDir)) {
      logger.warn(`Attempted to rename outside upload directory: ${newPath}`);
      return res.status(403).json({ error: 'Invalid destination path' });
    }
    
    // Check if a file/directory with the new name already exists
    try {
      await fs.access(newPath);
      return res.status(409).json({ error: 'A file or directory with that name already exists' });
    } catch (err) {
      // File doesn't exist, which is what we want
      if (err.code !== 'ENOENT') {
        throw err;
      }
    }
    
    // Perform the rename operation
    await fs.rename(currentPath, newPath);
    
    // Log the operation
    const itemType = stats.isDirectory() ? 'Directory' : 'File';
    logger.info(`${itemType} renamed: "${req.params[0]}" -> "${sanitizedNewName}"`);
    
    // Calculate relative path for response
    const relativePath = path.relative(config.uploadDir, newPath).replace(/\\/g, '/');
    
    res.json({ 
      message: `${itemType} renamed successfully`,
      oldName: path.basename(req.params[0]),
      newName: sanitizedNewName,
      newPath: relativePath
    });
    
  } catch (err) {
    logger.error(`Rename failed: ${err.message}`);
    res.status(err.code === 'ENOENT' ? 404 : 500).json({ 
      error: err.code === 'ENOENT' ? 'File or directory not found' : 'Failed to rename item' 
    });
  }
});

module.exports = router; 