def update_progress(progress_value, status_message, progress_bar, status_text):
    """
    Update the Streamlit progress bar and status message.
    
    Args:
        progress_value (float): Progress value between 0 and 1
        status_message (str): Status message to display
        progress_bar: Streamlit progress bar widget
        status_text: Streamlit text widget for status updates
    """
    progress_bar.progress(progress_value)
    status_text.text(status_message)
