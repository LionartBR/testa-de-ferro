import { useState, useCallback } from "react";

interface GrafoExpansionResult {
  expandedNodes: Set<string>;
  toggleNode: (nodeId: string) => void;
  resetExpansion: () => void;
}

export function useGrafoExpansion(): GrafoExpansionResult {
  const [expandedNodes, setExpandedNodes] = useState<Set<string>>(new Set());

  const toggleNode = useCallback((nodeId: string) => {
    setExpandedNodes((previous) => {
      const next = new Set(previous);
      if (next.has(nodeId)) {
        next.delete(nodeId);
      } else {
        next.add(nodeId);
      }
      return next;
    });
  }, []);

  const resetExpansion = useCallback(() => {
    setExpandedNodes(new Set());
  }, []);

  return { expandedNodes, toggleNode, resetExpansion };
}
