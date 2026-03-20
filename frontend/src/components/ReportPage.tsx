import React, { useState } from 'react';
import { useKeycloak } from '@react-keycloak/web';

interface ReportRow {
  user_id: number;
  user_name: string;
  user_email: string;
  prosthesis_type: string;
  total_signals: number;
  avg_frequency: number;
  avg_amplitude: number;
  avg_duration: number;
  max_amplitude: number;
  min_amplitude: number;
  first_signal_time: string;
  last_signal_time: string;
  etl_date: string;
}

interface ReportResponse {
  user_id: number;
  user_email: string;
  message: string;
  data: ReportRow[];
}

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [report, setReport] = useState<ReportResponse | null>(null);

  const fetchReport = async () => {
    // Проверка аутентификации — неаутентифицированный пользователь не может
    // получить отчёт
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);
      setReport(null);

      const response = await fetch(`${process.env.REACT_APP_API_URL}/reports`, {
        headers: {
          // Передаём токен Keycloak — бэкенд верифицирует его и возвращает
          // только данные текущего пользователя
          'Authorization': `Bearer ${keycloak.token}`,
        },
      });

      if (!response.ok) {
        const err = await response.json().catch(() => ({ detail: response.statusText }));
        throw new Error(err.detail || `HTTP ${response.status}`);
      }

      const data: ReportResponse = await response.json();
      setReport(data);
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const downloadJson = () => {
    if (!report) return;
    const blob = new Blob([JSON.stringify(report, null, 2)], {
      type: 'application/json',
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `prosthesis_report_${report.user_id}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  // Пока SDK не инициализировался
  if (!initialized) {
    return (
      <div className="flex items-center justify-center min-h-screen bg-gray-100">
        <div className="text-gray-500">Loading...</div>
      </div>
    );
  }

  // Пользователь не вошёл — показываем только кнопку логина
  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <div className="p-8 bg-white rounded-lg shadow-md text-center">
          <h1 className="text-2xl font-bold mb-4">Prosthesis Usage Reports</h1>
          <p className="text-gray-600 mb-6">Please log in to access your report.</p>
          <button
            onClick={() => keycloak.login()}
            className="px-6 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
          >
            Login
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100 p-4">
      <div className="w-full max-w-4xl p-8 bg-white rounded-lg shadow-md">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">Prosthesis Usage Reports</h1>
          <div className="flex items-center gap-3">
            <span className="text-sm text-gray-500">{keycloak.tokenParsed?.email}</span>
            <button
              onClick={() => keycloak.logout()}
              className="px-3 py-1 text-sm bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
            >
              Logout
            </button>
          </div>
        </div>

        {/* Кнопка загрузки отчёта */}
        <div className="flex gap-3 mb-6">
          <button
            onClick={fetchReport}
            disabled={loading}
            className={`px-5 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
              loading ? 'opacity-50 cursor-not-allowed' : ''
            }`}
          >
            {loading ? 'Loading Report...' : 'Download Report'}
          </button>

          {report && report.data.length > 0 && (
            <button
              onClick={downloadJson}
              className="px-5 py-2 bg-green-500 text-white rounded hover:bg-green-600"
            >
              Save as JSON
            </button>
          )}
        </div>

        {/* Ошибка */}
        {error && (
          <div className="mb-4 p-4 bg-red-100 text-red-700 rounded">
            <strong>Error:</strong> {error}
          </div>
        )}

        {/* Сообщение от сервера (например, данных ещё нет) */}
        {report && (
          <div className="mb-4 p-3 bg-blue-50 text-blue-700 rounded text-sm">
            {report.message}
          </div>
        )}

        {/* Таблица отчёта */}
        {report && report.data.length > 0 && (
          <div className="overflow-x-auto">
            <table className="w-full text-sm border-collapse">
              <thead>
                <tr className="bg-gray-50 border-b border-gray-200">
                  <th className="text-left p-3 font-semibold">Prosthesis</th>
                  <th className="text-right p-3 font-semibold">Signals</th>
                  <th className="text-right p-3 font-semibold">Avg Freq (Hz)</th>
                  <th className="text-right p-3 font-semibold">Avg Ampl</th>
                  <th className="text-right p-3 font-semibold">Max Ampl</th>
                  <th className="text-right p-3 font-semibold">Avg Duration (ms)</th>
                  <th className="text-left p-3 font-semibold">First Signal</th>
                  <th className="text-left p-3 font-semibold">Last Signal</th>
                  <th className="text-left p-3 font-semibold">ETL Date</th>
                </tr>
              </thead>
              <tbody>
                {report.data.map((row, idx) => (
                  <tr
                    key={idx}
                    className={`border-b border-gray-100 ${idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}`}
                  >
                    <td className="p-3 capitalize font-medium">{row.prosthesis_type}</td>
                    <td className="p-3 text-right">{row.total_signals.toLocaleString()}</td>
                    <td className="p-3 text-right">{row.avg_frequency}</td>
                    <td className="p-3 text-right">{row.avg_amplitude}</td>
                    <td className="p-3 text-right">{row.max_amplitude}</td>
                    <td className="p-3 text-right">{row.avg_duration}</td>
                    <td className="p-3 text-gray-600">{row.first_signal_time}</td>
                    <td className="p-3 text-gray-600">{row.last_signal_time}</td>
                    <td className="p-3 text-gray-500">{row.etl_date}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;
