'use client';

import { useState, useEffect } from 'react';

interface DataRow {
  count: number | string;
  age: number | string;
  sex: string;
  zip_code: string;
  library_friend: string;
  is_at_risk?: boolean;
  is_below_threshold?: boolean;
}

interface Stats {
  total_cells: number;
  redacted_cells: number;
  non_anonymous_cells: number;
  suppression_rate: number;
  threshold_used: number;
  dimension_suppressed: string;
}

export default function Home() {
  const [rawData, setRawData] = useState<DataRow[]>([]);
  const [suppressedData, setSuppressedData] = useState<DataRow[]>([]);
  const [stats, setStats] = useState<Stats | null>(null);
  const [threshold, setThreshold] = useState(11);
  const [redactedDimension, setRedactedDimension] = useState('sex');
  const [error, setError] = useState<string | null>(null);

  // Load both raw and suppressed data
  const loadData = async () => {
    try {
      // Load raw data with risk flags
      const rawResponse = await fetch(`/api/suppress-data?raw_data=true&threshold=${threshold}&redacted_dimension=${redactedDimension}`);
      const rawResult = await rawResponse.json();
      if (rawResult.success && rawResult.data) {
        setRawData(rawResult.data);
      }

      // Load suppressed data
      const suppressedResponse = await fetch(`/api/suppress-data?threshold=${threshold}&redacted_dimension=${redactedDimension}`);
      const suppressedResult = await suppressedResponse.json();
      if (suppressedResult.success && suppressedResult.data) {
        setSuppressedData(suppressedResult.data);
        if (suppressedResult.stats) {
          setStats(suppressedResult.stats);
        }
      }
    } catch (err) {
      console.error('Failed to load data:', err);
      setError('Failed to load data from API');
    }
  };

  // Load data on mount and when threshold/dimension changes
  useEffect(() => {
    loadData();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [threshold, redactedDimension]);

  return (
    <div className="min-h-screen bg-gray-50 py-8 px-4 text-gray-900">
      <div className="max-w-7xl mx-auto">
        <header className="text-center mb-8">
          <h1 className="text-4xl font-bold text-gray-900 mb-4">
            🥚 Ostrich Egg
          </h1>
          <p className="text-lg text-gray-600 max-w-3xl mx-auto">
            Aggregation Engine for Small-Cell Suppression - Protecting data privacy by
            identifying and suppressing cells that could reveal sensitive information.
          </p>
          <div className="mt-4">
            <a
              href="https://github.com/greenriver/ostrich-egg"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 text-gray-700 hover:text-gray-900 transition-colors"
            >
              <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 24 24" aria-hidden="true">
                <path fillRule="evenodd" d="M12 2C6.477 2 2 6.484 2 12.017c0 4.425 2.865 8.18 6.839 9.504.5.092.682-.217.682-.483 0-.237-.008-.868-.013-1.703-2.782.605-3.369-1.343-3.369-1.343-.454-1.158-1.11-1.466-1.11-1.466-.908-.62.069-.608.069-.608 1.003.07 1.531 1.032 1.531 1.032.892 1.53 2.341 1.088 2.91.832.092-.647.35-1.088.636-1.338-2.22-.253-4.555-1.113-4.555-4.951 0-1.093.39-1.988 1.029-2.688-.103-.253-.446-1.272.098-2.65 0 0 .84-.27 2.75 1.026A9.564 9.564 0 0112 6.844c.85.004 1.705.115 2.504.337 1.909-1.296 2.747-1.027 2.747-1.027.546 1.379.202 2.398.1 2.651.64.7 1.028 1.595 1.028 2.688 0 3.848-2.339 4.695-4.566 4.943.359.309.678.92.678 1.855 0 1.338-.012 2.419-.012 2.747 0 .268.18.58.688.482A10.019 10.019 0 0022 12.017C22 6.484 17.522 2 12 2z" clipRule="evenodd" />
              </svg>
              View on GitHub
            </a>
          </div>
        </header>

        {/* Controls */}
        <div className="bg-white p-6 rounded-lg shadow-sm mb-8">
          <h2 className="text-xl font-semibold mb-4">Configuration</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium mb-2">
                Privacy Threshold
              </label>
              <input
                type="number"
                value={threshold}
                onChange={(e) => setThreshold(Number(e.target.value))}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
                min="1"
              />
              <p className="text-xs text-gray-500 mt-1">
                Cells below this count are considered &quot;small&quot; and may be suppressed
              </p>
            </div>
            <div>
              <label className="block text-sm font-medium mb-2">
                Redaction Dimension
              </label>
              <select
                value={redactedDimension}
                onChange={(e) => setRedactedDimension(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
              >
                <option value="sex">Sex</option>
                <option value="age">Age</option>
                <option value="zip_code">Zip Code</option>
                <option value="library_friend">Library Friend</option>
              </select>
            </div>
          </div>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded-md mb-8">
            Error: {error}
          </div>
        )}

        {/* Data Comparison */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-8 mb-8">
          {/* Original Data */}
          <div className="bg-white p-6 rounded-lg shadow-sm">
            <h2 className="text-xl font-semibold mb-4 text-red-600">⚠️ Without Ostrich Egg</h2>
            <p className="text-sm text-gray-600 mb-4">
              Publishing raw data exposes small groups that could be easily identified.
              <strong className="text-red-600"> Highlighted rows show privacy risks</strong> where individuals could be identified through their unique characteristics or by combining data points.
            </p>
            <div className="overflow-x-auto">
              <table className="min-w-full border-collapse border border-gray-300 text-sm">
                <thead>
                  <tr className="bg-gray-50">
                    <th className="border border-gray-300 px-3 py-2 text-left">Count</th>
                    <th className="border border-gray-300 px-3 py-2 text-left">Age</th>
                    <th className="border border-gray-300 px-3 py-2 text-left">Sex</th>
                    <th className="border border-gray-300 px-3 py-2 text-left">Zip</th>
                    <th className="border border-gray-300 px-3 py-2 text-left">Friend</th>
                  </tr>
                </thead>
                <tbody>
                  {rawData.map((row, index) => (
                    <tr key={index} className={row.is_at_risk ? 'bg-red-50' : ''}>
                      <td className="border border-gray-300 px-3 py-2">
                        <div className="flex items-center">
                          {row.count}
                          {row.is_at_risk && (
                            <span className="ml-2 text-xs text-red-600 font-bold">
                              🚨 RISK
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="border border-gray-300 px-3 py-2">{row.age}</td>
                      <td className="border border-gray-300 px-3 py-2">{row.sex}</td>
                      <td className="border border-gray-300 px-3 py-2">{row.zip_code}</td>
                      <td className="border border-gray-300 px-3 py-2">{row.library_friend}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
            
            {/* Raw Data Risk Warning */}
            <div className="mt-4 p-3 bg-red-50 border border-red-200 rounded-lg">
              <div className="flex items-start">
                <div className="text-red-500 text-lg mr-2">⚠️</div>
                <div className="text-sm">
                  <strong className="text-red-700">Privacy Risk:</strong> Highlighted rows can be used to identify individuals,
                  violating privacy requirements like HIPAA. Small groups and unique combinations make people identifiable.
                </div>
              </div>
            </div>
          </div>

          {/* Protected Data */}
          <div className="bg-white p-6 rounded-lg shadow-sm">
            <h2 className="text-xl font-semibold mb-4 text-green-600">🛡️ With Ostrich Egg</h2>
            <p className="text-sm text-gray-600 mb-4">
              Ostrich Egg automatically identifies and suppresses at-risk data.
              <strong className="text-green-600"> Sensitive cells are &quot;Redacted&quot;</strong> to prevent both direct identification and indirect discovery through data subtraction.
            </p>
            
            {suppressedData.length > 0 ? (
              <>
                <div className="overflow-x-auto">
                  <table className="min-w-full border-collapse border border-gray-300 text-sm">
                    <thead>
                      <tr className="bg-gray-50">
                        <th className="border border-gray-300 px-3 py-2 text-left">Count</th>
                        <th className="border border-gray-300 px-3 py-2 text-left">Age</th>
                        <th className="border border-gray-300 px-3 py-2 text-left">Sex</th>
                        <th className="border border-gray-300 px-3 py-2 text-left">Zip</th>
                        <th className="border border-gray-300 px-3 py-2 text-left">Friend</th>
                      </tr>
                    </thead>
                    <tbody>
                      {suppressedData.map((row, index) => (
                        <tr key={index} className={row.count === 'Redacted' ? 'bg-red-50' : ''}>
                          <td className="border border-gray-300 px-3 py-2">{row.count}</td>
                          <td className="border border-gray-300 px-3 py-2">{row.age}</td>
                          <td className="border border-gray-300 px-3 py-2">{row.sex}</td>
                          <td className="border border-gray-300 px-3 py-2">{row.zip_code}</td>
                          <td className="border border-gray-300 px-3 py-2">{row.library_friend}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
                
                {/* Protection Success Message */}
                <div className="mt-4 p-3 bg-green-50 border border-green-200 rounded-lg">
                  <div className="flex items-start">
                    <div className="text-green-500 text-lg mr-2">🛡️</div>
                    <div className="text-sm">
                      <strong className="text-green-700">Privacy Protected:</strong> Ostrich Egg suppressed {stats?.redacted_cells || 0} cells
                      to prevent identification while preserving useful data for analysis.
                    </div>
                  </div>
                </div>
              </>
            ) : (
              <div className="flex items-center justify-center h-64 border-2 border-dashed border-gray-300 rounded-lg">
                <div className="text-center text-gray-500">
                  <div className="text-6xl mb-4">🥚</div>
                  <div className="text-lg font-medium">Click &quot;Run Suppression&quot; to see protected data</div>
                  <div className="text-sm">Ostrich Egg will analyze and secure the raw data above</div>
                </div>
              </div>
            )}
          </div>
        </div>

      </div>
    </div>
  );
}
