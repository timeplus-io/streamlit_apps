import streamlit as st
from PIL import Image

st.set_page_config(
    page_title="Timeplus Demos",
    page_icon="👋",
)

st.write("# Welcome to Timeplus Demos! 👋")

st.sidebar.success("Select a demo above.")

st.markdown("Timeplus is a fast and powerful real-time analytics platform.")
st.image(Image.open("tp_overview.png"))

st.markdown(
    """
    **👈 Select a demo from the sidebar** to see some examples
    of what Timeplus can do!
    ### Want to learn more?
    - Check out [timeplus.com](https://timeplus.com)
    - Jump into our [documentation](https://docs.timeplus.com)
    - Ask a question in our [community
        slack](https://join.slack.com/t/timepluscommunity/shared_invite/zt-14nymxet0-9_Hxszyi5fXUL0Ra_lI~lw)
"""
)