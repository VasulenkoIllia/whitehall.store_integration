const express = require('express');
const { port, adminLogin, adminPassword } = require('./config');
const logger = require('./logger');
const logService = require('./services/logService');
const healthRouter = require('./routes/health');
const jobsRouter = require('./routes/jobs');
const adminRouter = require('./routes/admin');
const path = require('path');
const fs = require('fs');
const { startScheduler } = require('./jobs/scheduler');

const app = express();
app.use(express.json({ limit: '2mb' }));

function parseBasicAuth(header) {
  if (!header || !header.startsWith('Basic ')) {
    return null;
  }
  const encoded = header.slice(6).trim();
  if (!encoded) {
    return null;
  }
  const decoded = Buffer.from(encoded, 'base64').toString('utf8');
  const [user, pass] = decoded.split(':');
  if (!user && !pass) {
    return null;
  }
  return { user, pass };
}

function requireAuth(req, res, next) {
  if (req.path.startsWith('/health')) {
    return next();
  }
  if (!adminLogin || !adminPassword) {
    return next();
  }
  const auth = parseBasicAuth(req.headers.authorization);
  if (auth && auth.user === adminLogin && auth.pass === adminPassword) {
    return next();
  }
  res.set('WWW-Authenticate', 'Basic realm="Admin"');
  return res.status(401).send('Unauthorized');
}

app.use(requireAuth);

app.use('/health', healthRouter);
app.use('/jobs', jobsRouter);
app.use('/admin/api', adminRouter);

const adminDistPath = path.join(__dirname, '..', 'admin-ui', 'dist');
const adminUiReady = fs.existsSync(adminDistPath);

if (adminUiReady) {
  app.use('/admin', express.static(adminDistPath));
  app.get('/admin/*', (req, res) => {
    res.sendFile(path.join(adminDistPath, 'index.html'));
  });
  app.use('/', express.static(adminDistPath));
  app.get('/*', (req, res) => {
    res.sendFile(path.join(adminDistPath, 'index.html'));
  });
} else {
  app.get('/admin', (req, res) => {
    res
      .status(503)
      .send('Admin UI is not built. Run `npm run admin:build` or `npm run admin:dev`.');
  });
  app.get('/admin/*', (req, res) => {
    res
      .status(503)
      .send('Admin UI is not built. Run `npm run admin:build` or `npm run admin:dev`.');
  });
}

app.use((err, req, res, next) => {
  logger.error({ err }, 'Unhandled error');
  if (req.path.startsWith('/admin/api')) {
    logService
      .log(null, 'error', 'Admin API error', {
        path: req.path,
        method: req.method,
        error: err.message
      })
      .catch(() => {});
  }
  const status = err.status || err.statusCode || 500;
  res.status(status).json({ error: err.message || 'internal_error' });
});

app.listen(port, () => {
  logger.info({ port }, 'API started');
  startScheduler();
});
