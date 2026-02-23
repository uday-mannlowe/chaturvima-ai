import faiss
import re
from typing import List, Dict
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.documents import Document
from langchain_community.embeddings import HuggingFaceEmbeddings
from langchain_community.vectorstores import FAISS
from langchain_community.docstore.in_memory import InMemoryDocstore


class SemanticStageChunker:
    """
    Chunks documents based on behavioral stages with semantic understanding.
    """
    
    def __init__(self):
        self.stage_patterns = [
            r"Honeymoon Stage:",
            r"Self-Reflection Stage:",
            r"Soul Searching Stage:",
            r"Steady State Stage:"
        ]
    
    def extract_stages(self, text: str) -> List[Dict[str, str]]:
        """
        Extract behavioral stages from the document.
        
        Args:
            text: Full document text
            
        Returns:
            List of dictionaries containing stage information
        """
        chunks = []
        
        # Find all stage headers and their positions
        stage_positions = []
        for pattern in self.stage_patterns:
            matches = re.finditer(pattern, text)
            for match in matches:
                stage_name = match.group().replace(":", "").strip()
                stage_positions.append((match.start(), stage_name))
        
        # Sort by position
        stage_positions.sort(key=lambda x: x[0])
        
        # Extract content for each stage
        for i, (start_pos, stage_name) in enumerate(stage_positions):
            # Find end position (start of next stage or end of document)
            if i < len(stage_positions) - 1:
                end_pos = stage_positions[i + 1][0]
            else:
                end_pos = len(text)
            
            # Extract stage content
            stage_content = text[start_pos:end_pos].strip()
            
            # Further split into sub-sections (bullet points)
            sub_chunks = self._extract_sub_sections(stage_content, stage_name)
            chunks.extend(sub_chunks)
        
        return chunks
    
    def _extract_sub_sections(self, stage_content: str, stage_name: str) -> List[Dict[str, str]]:
        """
        Extract sub-sections (bullet points) from a stage.
        
        Args:
            stage_content: Content of a single stage
            stage_name: Name of the stage
            
        Returns:
            List of sub-section chunks
        """
        chunks = []
        
        # Split by bullet points
        lines = stage_content.split('\n')
        
        # Extract main description (before first bullet)
        main_desc = []
        sub_sections = []
        current_section = []
        
        in_main_desc = True
        
        for line in lines:
            line = line.strip()
            if not line:
                continue
            
            # Check if it's a bullet point
            if line.startswith('•'):
                in_main_desc = False
                if current_section:
                    sub_sections.append('\n'.join(current_section))
                current_section = [line]
            else:
                if in_main_desc:
                    main_desc.append(line)
                else:
                    current_section.append(line)
        
        # Add last section
        if current_section:
            sub_sections.append('\n'.join(current_section))
        
        # Create main stage chunk
        if main_desc:
            chunks.append({
                'content': '\n'.join(main_desc),
                'stage': stage_name,
                'section_type': 'overview',
                'section_name': f"{stage_name} - Overview"
            })
        
        # Create sub-section chunks
        for idx, sub_section in enumerate(sub_sections):
            # Extract sub-section title from first line
            first_line = sub_section.split('\n')[0]
            sub_title = first_line.replace('•', '').strip().split(':')[0]
            
            chunks.append({
                'content': sub_section,
                'stage': stage_name,
                'section_type': 'sub_section',
                'section_name': f"{stage_name} - {sub_title}"
            })
        
        return chunks


class EnhancedRAGPipeline:
    """
    Enhanced RAG pipeline with semantic stage-level chunking.
    """
    
    def __init__(self, embedding_model: str = "sentence-transformers/all-MiniLM-L6-v2"):
        self.chunker = SemanticStageChunker()
        self.embeddings = HuggingFaceEmbeddings(model_name=embedding_model)
        self.vectorstore = None
    
    def load_and_chunk_pdf(self, pdf_path: str) -> List[Document]:
        """
        Load PDF and create semantic chunks.
        
        Args:
            pdf_path: Path to PDF file
            
        Returns:
            List of chunked documents
        """
        # Load PDF
        loader = PyPDFLoader(pdf_path)
        pages = loader.load()
        
        # Combine all pages
        full_text = "\n\n".join(page.page_content for page in pages)
        
        # Extract semantic chunks
        chunks = self.chunker.extract_stages(full_text)
        
        # Create Document objects
        documents = []
        for idx, chunk in enumerate(chunks):
            doc = Document(
                page_content=chunk['content'],
                metadata={
                    'source': pdf_path,
                    'chunk_id': idx,
                    'stage': chunk['stage'],
                    'section_type': chunk['section_type'],
                    'section_name': chunk['section_name']
                }
            )
            documents.append(doc)
        
        print(f"Created {len(documents)} semantic chunks from {len(pages)} pages")
        return documents
    
    def build_vectorstore(self, documents: List[Document]) -> FAISS:
        """
        Build FAISS vector store from documents.
        
        Args:
            documents: List of chunked documents
            
        Returns:
            FAISS vector store
        """
        # Get embedding dimension
        embedding_dim = len(self.embeddings.embed_query("test"))
        
        # Create FAISS index
        index = faiss.IndexFlatL2(embedding_dim)
        
        # Initialize vector store
        self.vectorstore = FAISS(
            embedding_function=self.embeddings,
            index=index,
            docstore=InMemoryDocstore(),
            index_to_docstore_id={}
        )
        
        # Add documents
        self.vectorstore.add_documents(documents)
        
        print(f"Vector store created with {self.vectorstore.index.ntotal} vectors")
        return self.vectorstore
    
    def save_vectorstore(self, path: str):
        """Save vector store to disk."""
        if self.vectorstore is None:
            raise ValueError("No vector store to save. Build it first.")
        
        self.vectorstore.save_local(path)
        print(f"Vector store saved to {path}")
    
    def load_vectorstore(self, path: str):
        """Load vector store from disk."""
        self.vectorstore = FAISS.load_local(
            path,
            self.embeddings,
            allow_dangerous_deserialization=True
        )
        print(f"Vector store loaded from {path}")
        return self.vectorstore
    
    def query(self, question: str, k: int = 3) -> List[Dict]:
        """
        Query the vector store.
        
        Args:
            question: Query string
            k: Number of results to return
            
        Returns:
            List of relevant documents with metadata
        """
        if self.vectorstore is None:
            raise ValueError("No vector store available. Build or load one first.")
        
        retriever = self.vectorstore.as_retriever(search_kwargs={"k": k})
        docs = retriever.invoke(question)
        
        results = []
        for doc in docs:
            results.append({
                'content': doc.page_content,
                'metadata': doc.metadata
            })
        
        return results
    
    def display_results(self, results: List[Dict]):
        """Display query results in a readable format."""
        print(f"\n{'='*80}")
        print(f"Found {len(results)} relevant chunks:")
        print(f"{'='*80}\n")
        
        for idx, result in enumerate(results, 1):
            metadata = result['metadata']
            print(f"Result {idx}:")
            print(f"Stage: {metadata['stage']}")
            print(f"Section: {metadata['section_name']}")
            print(f"Type: {metadata['section_type']}")
            print(f"\nContent:\n{result['content'][:300]}...")
            print(f"\n{'-'*80}\n")


# Example usage
if __name__ == "__main__":
    # Initialize pipeline
    rag = EnhancedRAGPipeline()
    
    # Load and chunk PDF
    documents = rag.load_and_chunk_pdf("rag/behavioral_stages.pdf")
    
    # Build vector store
    rag.build_vectorstore(documents)
    
    # Save vector store
    rag.save_vectorstore("vector_db/behavioral_stages_semantic")
    
    # Example queries
    queries = [
        "Person is very confident and relies on past success",
        "Someone is struggling but willing to change",
        "Team is stable and meeting goals consistently",
        "People are blaming others for problems"
    ]
    
    for query in queries:
        print(f"\n\nQuery: '{query}'")
        results = rag.query(query, k=2)
        rag.display_results(results)