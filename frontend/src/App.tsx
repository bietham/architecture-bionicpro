import React from 'react';
import { ReactKeycloakProvider } from '@react-keycloak/web';
import Keycloak, { KeycloakConfig, KeycloakInitOptions } from 'keycloak-js';
import ReportPage from './components/ReportPage';

const keycloakConfig: KeycloakConfig = {
  url: process.env.REACT_APP_KEYCLOAK_URL,
  realm: process.env.REACT_APP_KEYCLOAK_REALM || "",
  clientId: process.env.REACT_APP_KEYCLOAK_CLIENT_ID || ""
};

const keycloak = new Keycloak(keycloakConfig);

/**
 * Инициализационные параметры Keycloak.
 *
 * pkceMethod: 'S256' — включает PKCE (Proof Key for Code Exchange) с SHA-256.
 * При каждом запросе на авторизацию библиотека автоматически:
 *   1. Генерирует случайный code_verifier.
 *   2. Вычисляет code_challenge = BASE64URL(SHA256(code_verifier)).
 *   3. Передаёт code_challenge и code_challenge_method=S256 в authorization request.
 *   4. При обмене кода на токен передаёт code_verifier — Keycloak проверяет соответствие.
 *
 * Это исключает возможность использования перехваченного authorization code
 * злоумышленником без знания code_verifier.
 *
 * onLoad: 'check-sso' — проверяет наличие существующей сессии без принудительного
 * редиректа на страницу входа.
 */
const keycloakInitOptions: KeycloakInitOptions = {
  pkceMethod: 'S256',
  onLoad: 'check-sso',
  silentCheckSsoRedirectUri: window.location.origin + '/silent-check-sso.html',
};

const App: React.FC = () => {
  return (
    <ReactKeycloakProvider
      authClient={keycloak}
      initOptions={keycloakInitOptions}
    >
      <div className="App">
        <ReportPage />
      </div>
    </ReactKeycloakProvider>
  );
};

export default App;
