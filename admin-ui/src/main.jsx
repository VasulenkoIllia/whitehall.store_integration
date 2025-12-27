import React from 'react';
import { createRoot } from 'react-dom/client';
import { ConfigProvider, App as AntApp } from 'antd';
import App from './App.jsx';
import 'antd/dist/reset.css';
import './styles.css';

const root = createRoot(document.getElementById('root'));

root.render(
  <ConfigProvider
    theme={{
      token: {
        colorPrimary: '#2563eb',
        colorInfo: '#2563eb',
        colorSuccess: '#16a34a',
        colorWarning: '#f59e0b',
        colorError: '#dc2626',
        fontFamily: '"Plus Jakarta Sans", "Segoe UI", sans-serif',
        borderRadius: 10
      }
    }}
  >
    <AntApp>
      <App />
    </AntApp>
  </ConfigProvider>
);
